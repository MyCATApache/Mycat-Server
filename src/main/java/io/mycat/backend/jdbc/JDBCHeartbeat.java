package io.mycat.backend.jdbc;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.MySQLHeartbeat;
import io.mycat.config.model.DataHostConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.statistic.HeartbeatRecorder;

public class JDBCHeartbeat extends DBHeartbeat{
	public static final Logger LOGGER = LoggerFactory.getLogger(JDBCHeartbeat.class);

	private final ReentrantLock lock;
	private final JDBCDatasource source;
    private final boolean heartbeatnull;
    private Long lastSendTime = System.currentTimeMillis();
    private Long lastReciveTime = System.currentTimeMillis();

	private final int maxRetryCount;

	private Logger logger = LoggerFactory.getLogger(this.getClass());
    
	public JDBCHeartbeat(JDBCDatasource source)
	{
		this.source = source;
		lock = new ReentrantLock(false);
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHearbeatSQL().trim();
		this.heartbeatnull= heartbeatSQL.length()==0;
		this.maxRetryCount = source.getHostConfig().getMaxRetryCount();

	}

	@Override
	public void start()
	{
		if (this.heartbeatnull){
			stop();
			return;
		}
		lock.lock();
		try
		{
			isStop.compareAndSet(true, false);
			this.status = DBHeartbeat.OK_STATUS;
		} finally
		{
			lock.unlock();
		}
	}

	@Override
	public void stop()
	{
		lock.lock();
		try
		{
			if (isStop.compareAndSet(false, true))
			{
				isChecking.set(false);
			}
		} finally
		{
			lock.unlock();
		}
	}

	@Override
	public String getLastActiveTime()
	{
	    long t = lastReciveTime;
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(t));
	}

	@Override
	public long getTimeout()
	{
		return 0;
	}
	@Override
	public HeartbeatRecorder getRecorder() {
        recorder.set(lastReciveTime - lastSendTime);
        return recorder;
    }
	
	@Override
	public void heartbeat()
	{
	    
		if (isStop.get()) {
			return;
		}
		lastSendTime = System.currentTimeMillis();
		lock.lock();
		try
		{
			isChecking.set(true);
			try (Connection c = source.getConnection())
			{
				try (Statement s = c.createStatement())
				{
					s.execute(heartbeatSQL);
				}
				c.close();
			}
			setResult(OK_STATUS);
			if(logger.isDebugEnabled()){
			    logger.debug("JDBCHeartBeat con query sql: "+heartbeatSQL);
			}
			
		} catch (Exception ex)
		{
		    logger.error("JDBCHeartBeat error",ex);
//			status = ERROR_STATUS;
			setResult(ERROR_STATUS);
		} finally
		{
			lock.unlock();
			this.isChecking.set(false);
			lastReciveTime = System.currentTimeMillis();
		}
	}

	public void setResult(int result) {
		switch (result) {
			case OK_STATUS:
				setOk();
				break;
			case ERROR_STATUS:
				setError();
				break;
			case TIMEOUT_STATUS:
				setTimeout();
				break;
		}
		if (this.status != OK_STATUS) {
			switchSourceIfNeed("heartbeat error");
		}

	}

	private void setOk() {
		switch (status) {
			case DBHeartbeat.TIMEOUT_STATUS:
				writeStatusMsg(source.getDbPool().getHostName(), source.getName() ,DBHeartbeat.INIT_STATUS);
				this.status = DBHeartbeat.INIT_STATUS;
				this.errorCount.set(0);
				//前一个状态为超时 当前状态为正常状态  那就马上发送一个请求 来验证状态是否恢复为Ok
				heartbeat();// timeout, heart beat again
				break;
			case DBHeartbeat.OK_STATUS:
				this.errorCount.set(0);
				break;
			default:
				writeStatusMsg(source.getDbPool().getHostName(), source.getName() ,DBHeartbeat.OK_STATUS);
				this.status = OK_STATUS;
				this.errorCount.set(0);;
		}
	}
	//发生错误了,是否进行下一次心跳检测的策略 . 是否进行下一次心跳检测.
	private void nextDector( int nextStatue) {

		if (isStop.get()) {
			writeStatusMsg(source.getDbPool().getHostName(), source.getName() ,DBHeartbeat.OK_STATUS);
			this.status = nextStatue;
		} else {
			// should continues check error status
			if(errorCount.get() < maxRetryCount) {
			} else {
				writeStatusMsg(source.getDbPool().getHostName(), source.getName() ,nextStatue);
				this.status = nextStatue;
				this.errorCount.set(0);
			}
		}
	}


	private void setError() {
		errorCount.incrementAndGet() ;
		nextDector( ERROR_STATUS);
	}

	private void setTimeout() {
		errorCount.incrementAndGet() ;
		nextDector( TIMEOUT_STATUS);
		//status = DBHeartbeat.TIMEOUT_STATUS;
	}

	/**
	 * switch data source
	 */
	private void switchSourceIfNeed(String reason) {
		int switchType = source.getHostConfig().getSwitchType();
		String notSwitch = source.getHostConfig().getNotSwitch();
		if (notSwitch.equals(DataHostConfig.FOVER_NOT_SWITCH_DS)
				|| switchType == DataHostConfig.NOT_SWITCH_DS) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("not switch datasource ,for switchType is "
						+ DataHostConfig.NOT_SWITCH_DS);
				return;
			}
			return;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("to  switchSourceIfNeed function 进行读节点转换 "
			);
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
									pool.switchSourceOrVoted(nextId, true, reason);
									break;
								} else {
									LOGGER.warn("ignored  datasource ,slave is not  synchronized to master , slave behind master :"
											+ theSourceHB.getSlaveBehindMaster()
											+ " " + theSource.getConfig());
								}
							} else {
								// normal switch
								LOGGER.info("try to switch datasource ,not checked slave synchronize status " + theSource.getConfig());
								pool.switchSourceOrVoted(nextId, true, reason);
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
