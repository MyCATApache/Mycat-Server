package io.mycat.backend.postgresql.heartbeat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.postgresql.PostgreSQLDataSource;
import io.mycat.config.model.DataHostConfig;

public class PostgreSQLHeartbeat extends DBHeartbeat {

	private static final int MAX_RETRY_COUNT = 5;

	public static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLHeartbeat.class);

	private PostgreSQLDataSource source;

	private ReentrantLock lock;

	private int maxRetryCount;

	private PostgreSQLDetector detector;

	public PostgreSQLHeartbeat(PostgreSQLDataSource source) {
		this.source = source;
		this.lock = new ReentrantLock(false);
		this.maxRetryCount = MAX_RETRY_COUNT;
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHearbeatSQL();
	}

	@Override
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

	@Override
	public void stop() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isStop.compareAndSet(false, true)) {
				if (isChecking.get()) {
					// nothing
				} else {
					PostgreSQLDetector detector = this.detector;
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

	@Override
	public String getLastActiveTime() {
		PostgreSQLDetector detector = this.detector;
		if (detector == null) {
			return null;
		}
		long t = detector.getLasstReveivedQryTime();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date(t));
	}

	@Override
	public long getTimeout() {
		PostgreSQLDetector detector = this.detector;
		if (detector == null) {
			return -1L;
		}
		return detector.getHeartbeatTimeout();
	}

	@Override
	public void heartbeat() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (isChecking.compareAndSet(false, true)) {
				PostgreSQLDetector detector = this.detector;
				if (detector == null || detector.isQuit()) {
					try {
						detector = new PostgreSQLDetector(this);
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
				PostgreSQLDetector detector = this.detector;
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

	public PostgreSQLDataSource getSource() {
		return source;
	}

	public void setResult(int result, PostgreSQLDetector detector, Object attr) {
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

	private void switchSourceIfNeed(String reason) {
		int switchType = source.getHostConfig().getSwitchType();
		if (switchType == DataHostConfig.NOT_SWITCH_DS) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("not switch datasource ,for switchType is " + DataHostConfig.NOT_SWITCH_DS);
				return;
			}
			return;
		}
		PhysicalDBPool pool = this.source.getDbPool();
		int curDatasourceHB = pool.getSource().getHeartbeat().getStatus();
		// read node can't switch ,only write node can switch
		if (pool.getWriteType() == PhysicalDBPool.WRITE_ONLYONE_NODE && !source.isReadNode()
				&& curDatasourceHB != DBHeartbeat.OK_STATUS && pool.getSources().length > 1) {
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
								if (Integer.valueOf(0).equals(theSourceHB.getSlaveBehindMaster())) {
									LOGGER.info("try to switch datasource ,slave is synchronized to master "
											+ theSource.getConfig());
									pool.switchSource(nextId, true, reason);
									break;
								} else {
									LOGGER.warn(
											"ignored  datasource ,slave is not  synchronized to master , slave behind master :"
													+ theSourceHB.getSlaveBehindMaster() + " " + theSource.getConfig());
								}
							} else {
								// normal switch
								LOGGER.info("try to switch datasource ,not checked slave synchronize status "
										+ theSource.getConfig());
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

	private void setTimeout(PostgreSQLDetector detector) {
		this.isChecking.set(false);
		status = DBHeartbeat.TIMEOUT_STATUS;
	}

	private void setError(PostgreSQLDetector detector) {
		// should continues check error status
		if (++errorCount < maxRetryCount) {

			if (detector != null && !detector.isQuit()) {
				heartbeat(); // error count not enough, heart beat again
			}
			// return;
		} else {
			if (detector != null) {
				detector.quit();
			}

			this.status = ERROR_STATUS;
			this.errorCount = 0;

		}
	}

	private void setOk(PostgreSQLDetector detector) {
		recorder.set(detector.getLasstReveivedQryTime() - detector.getLastSendQryTime());
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

}
