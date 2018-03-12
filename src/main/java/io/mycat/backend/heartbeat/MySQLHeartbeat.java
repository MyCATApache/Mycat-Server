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
package io.mycat.backend.heartbeat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.model.DataHostConfig;

/**
 * @author mycat
 */
public class MySQLHeartbeat extends DBHeartbeat {

	private static final int MAX_RETRY_COUNT = 5;
	public static final Logger LOGGER = LoggerFactory.getLogger(MySQLHeartbeat.class);

	private final MySQLDataSource source;

	private final ReentrantLock lock;
	private final int maxRetryCount;

	private MySQLDetector detector;

	public MySQLHeartbeat(MySQLDataSource source) {
		this.source = source;
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
		long t = detector.getLasstReveivedQryTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(t));
	}

	public void start() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			isStop.compareAndSet(true, false);
			super.status = DBHeartbeat.OK_STATUS;
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
				if (detector == null || detector.isQuit()) {
					try {
						detector = new MySQLDetector(this);
						detector.heartbeat();
					} catch (Exception e) {
						LOGGER.warn(source.getConfig().toString(), e);
						setResult(ERROR_STATUS, detector, null);
						return;
					}
					this.detector = detector;
				} else {
					detector.heartbeat();
				}
			} else {
				MySQLDetector detector = this.detector;
				if (detector != null) {
					if (detector.isQuit()) {
						isChecking.compareAndSet(true, false);
					} else if (detector.isHeartbeatTimeout()) {
						setResult(TIMEOUT_STATUS, detector, null);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void setResult(int result, MySQLDetector detector, String msg) {
		this.isChecking.set(false);
		switch (result) {
		case OK_STATUS:
			setOk(detector);
			break;
		case ERROR_STATUS:
			setError(detector);
			break;
		case TIMEOUT_STATUS:
			setTimeout(detector);
			break;
		}
		if (this.status != OK_STATUS) {
			switchSourceIfNeed("heartbeat error");
		}
	}

	private void setOk(MySQLDetector detector) {
		switch (status) {
		case DBHeartbeat.TIMEOUT_STATUS:
			this.status = DBHeartbeat.INIT_STATUS;
			this.errorCount = 0;
			if (isStop.get()) {
				detector.quit();
			} else {
				heartbeat();// timeout, heart beat again
			}
			break;
		case DBHeartbeat.OK_STATUS:
			break;
		default:
			this.status = OK_STATUS;
			this.errorCount = 0;
		}
		if (isStop.get()) {
			detector.quit();
		}
	}

	private void setError(MySQLDetector detector) {
		// should continues check error status
		if (++errorCount < maxRetryCount) {

            if (detector != null && !detector.isQuit()) {
                heartbeat(); // error count not enough, heart beat again
            }

		}else
        {
            if (detector != null ) {
                detector.quit();
            }
            this.status = ERROR_STATUS;
            this.errorCount = 0;
        }
	}

	private void setTimeout(MySQLDetector detector) {
		this.isChecking.set(false);
		status = DBHeartbeat.TIMEOUT_STATUS;
	}

	/**
	 * switch data source
	 */
	private void switchSourceIfNeed(String reason) {
		int switchType = source.getHostConfig().getSwitchType();
		if (switchType == DataHostConfig.NOT_SWITCH_DS) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("not switch datasource ,for switchType is "
						+ DataHostConfig.NOT_SWITCH_DS);
				return;
			}
			return;
		}
		PhysicalDBPool pool = this.source.getDbPool();
		int curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
		// read node can't switch ,only write node can switch
		if (pool.getWriteType() == PhysicalDBPool.WRITE_ONLYONE_NODE
				&& !source.isReadNode()
				&& curDatasourceHB != DBHeartbeat.OK_STATUS
				&& pool.getSources().length > 1) {
			synchronized (pool) {
				// try to see if need switch datasource
				curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
				if (curDatasourceHB != DBHeartbeat.INIT_STATUS && curDatasourceHB != DBHeartbeat.OK_STATUS) {
					int curIndex = pool.getActivedIndex();
					int nextId = pool.next(curIndex);
					PhysicalDatasource[] allWriteNodes = pool.getSources();
					while (true) {
						if (nextId == curIndex) {
							break;
						}
						PhysicalDatasource theSource = allWriteNodes[nextId];
						DBHeartbeat theSourceHB = theSource.getHeartbeat();
						int theSourceHBStatus = theSourceHB.getStatus();
						if (theSourceHBStatus == DBHeartbeat.OK_STATUS) {
							if (switchType == DataHostConfig.SYN_STATUS_SWITCH_DS) {
								if (Integer.valueOf(0).equals( theSourceHB.getSlaveBehindMaster())) {
									LOGGER.info("try to switch datasource ,slave is synchronized to master " + theSource.getConfig());
									pool.switchSource(nextId, true, reason);
									break;
								} else {
									LOGGER.warn("ignored  datasource ,slave is not  synchronized to master , slave behind master :"
											+ theSourceHB.getSlaveBehindMaster()
											+ " " + theSource.getConfig());
								}
							} else {
								// normal switch
								LOGGER.info("try to switch datasource ,not checked slave synchronize status " + theSource.getConfig());
								pool.switchSource(nextId, true, reason);
                                break;
							}

						}
						nextId = pool.next(nextId);
					}

				}
			}
		}
	}
}