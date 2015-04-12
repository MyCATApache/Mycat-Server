/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.heartbeat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.mysql.nio.MySQLDataSource;

/**
 * @author mycat
 */
public class MySQLHeartbeat extends DBHeartbeat {

	private static final int MAX_RETRY_COUNT = 5;
	private static final Logger LOGGER = Logger.getLogger(MySQLHeartbeat.class);

	private final MySQLDataSource source;

	private final MySQLDetectorFactory factory;

	private final ReentrantLock lock;
	private final int maxRetryCount;

	private MySQLDetector detector;

	public MySQLHeartbeat(MySQLDataSource source) {
		this.source = source;
		this.factory = new MySQLDetectorFactory();
		this.lock = new ReentrantLock(false);
		this.maxRetryCount = MAX_RETRY_COUNT;
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHearbeatSQL();
	}

	public MySQLDataSource getSource() {
		return source;
	}

	public MySQLDetector getDetector() {
		return detector;
	}

	public long getTimeout() {
		MySQLDetector detector = this.detector;
		if (detector == null) {
			return -1L;
		}
		return detector.getHeartbeatTimeout();
	}

	public String getLastActiveTime() {
		MySQLDetector detector = this.detector;
		if (detector == null) {
			return null;
		}
		long t = Math.max(detector.lastReadTime(), detector.lastWriteTime());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(t));
	}

	public void start() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			isStop.compareAndSet(true, false);
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isStop.compareAndSet(false, true)) {
				if (isChecking.get()) {
					// nothing
				} else {
					MySQLDetector detector = this.detector;
					if (detector != null) {
						detector.quit();
						isChecking.set(false);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * execute heart beat
	 */
	public void heartbeat() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isChecking.compareAndSet(false, true)) {
				MySQLDetector detector = this.detector;
				if (detector == null || detector.isQuit()
						|| detector.isClosed()) {
					try {
						detector = factory.make(this);
					} catch (Exception e) {
						LOGGER.warn(source.getConfig().toString(), e);
						setError(null);
						return;
					}
					this.detector = detector;
				} else {
					detector.heartbeat();
				}
			} else {
				MySQLDetector detector = this.detector;
				if (detector != null) {
					if (detector.isQuit() || detector.isClosed()) {
						isChecking.compareAndSet(true, false);
					} else if (detector.isHeartbeatTimeout()) {
						setTimeout(detector);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void setResult(int result, MySQLDetector detector,
			boolean isTransferError,String msg) {
		switch (result) {
		case OK_STATUS:
			setOk(detector);
			break;
		case ERROR_STATUS:
			if (detector.isQuit()) {
				isChecking.set(false);
			} else {
				if (isTransferError) {
					detector.close(msg);
				}
				setError(detector);
			}
			break;
		}
	}

	private void setOk(MySQLDetector detector) {

		recorder.set(detector.lastReadTime() - detector.lastWriteTime());
		switch (status) {
		case DBHeartbeat.TIMEOUT_STATUS:
			this.status = DBHeartbeat.INIT_STATUS;
			this.errorCount = 0;
			this.isChecking.set(false);
			if (isStop.get()) {
				detector.quit();
			} else {
				heartbeat();// timeout, heart beat again
			}
			break;
		default:
			this.status = OK_STATUS;
			this.errorCount = 0;
			this.isChecking.set(false);
			this.switchSourceIfNeed("heart beate ok");
			if (isStop.get()) {
				detector.quit();
			}
		}
	}

	private void setError(MySQLDetector detector) {
		// should continues check error status
		if (++errorCount < maxRetryCount) {
			isChecking.set(false);
			if (detector != null && isStop.get()) {
				detector.quit();
			} else {
				heartbeat(); // error count not enough, heart beat again
			}
			return;
		}

		this.status = ERROR_STATUS;
		this.errorCount = 0;
		this.isChecking.set(false);
	}

	private void setTimeout(MySQLDetector detector) {
		status = DBHeartbeat.TIMEOUT_STATUS;
		isChecking.set(false);

	}

	/**
	 * switch data source
	 */
	private void switchSourceIfNeed(String reason) {
		PhysicalDBPool pool = source.getDbPool();

		// read node can't switch ,only write node can switch
		if (pool.getWriteType() == PhysicalDBPool.WRITE_ONLYONE_NODE
				&& !source.isReadNode() && this.status == DBHeartbeat.OK_STATUS) {

			// try to see if need switch datasource
			int curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
			if (pool.getSources().length > 1
					&& curDatasourceHB != DBHeartbeat.INIT_STATUS
					&& curDatasourceHB != DBHeartbeat.OK_STATUS) {
				int myIndex = -1;
				PhysicalDatasource[] allWriteNodes = pool.getSources();
				for (int i = 0; i < allWriteNodes.length; i++) {
					if (this.source == allWriteNodes[i]) {
						myIndex = i;
						break;
					}
				}
				pool.switchSource(myIndex, true, reason);
			}
		}
	}
}