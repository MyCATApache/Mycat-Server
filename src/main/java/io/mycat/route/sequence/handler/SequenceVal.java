package io.mycat.route.sequence.handler;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.jdbc.JDBCDatasource;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.DataHostConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class SequenceVal {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SequenceVal.class);

	private static final String errSeqResult = "-999999,null";
	public AtomicLong curVal = new AtomicLong(0); //当前可用的id
	public volatile long maxSegValue; //最大的可用id

	public volatile String dbretVal = null; //数据库的返回结果
	public volatile boolean dbfinished ;//请求是否成功返回
	public AtomicBoolean fetching = new AtomicBoolean(false); //是否正在获取id序列当中。
	public volatile boolean successFetched; //是否已经成功的获取并且设置值了。
	public volatile String dataNode;//请求的分片节点
	public final String seqName;//那个表
	public final String sql;//请求分片的sql SELECT mycat_seq_nextval('tableName');
	//初始化
	public void reset(){

		dbretVal = null;
		dbfinished = false;
		successFetched = false; //添加代码

	}
	//设置成功返回
	public void setDbfinished() {
		dbfinished = true;
	}
	//
	public SequenceVal(String seqName, String dataNode) {
		this.seqName = seqName;
		this.dataNode = dataNode;
		sql = "SELECT mycat_seq_nextval('" + seqName + "')";
	}
	//nexVal是否可以用的判断
	public boolean isNexValValid(Long nexVal) {
		if (nexVal < this.maxSegValue) {
			return true;
		} else {
			return false;
		}
	}

	//设置当前的可用值。
	public void setCurValue(long newValue) {
		curVal.set(newValue);
	}
	// 重试次数 保证最大可能性的获取到。
	/*
	 *1.后端获取序列
	 *  如果返回的结果为超时，异常，尝试数据库重试获取
	 *     返回的为0，0   尝试数据库重试获取
	 *     结果还未返回，调用函数继续等待
	 *  成功返回则正常返回
	 * */

	public Long[] fetchSequenceFromDB(FetchMySQLSequnceHandler mysqlSeqFetcher, int retryCount, boolean canSendFetch) {
		DataHostConfig dataHostConfig = getDBHostConfig();

		if (dataHostConfig.isJDBCDriver()) {
			return getSequenceValueByJDBC(retryCount);
		}

		if (dataHostConfig.isNativeDriver()) {
			return getSequenceByNaitve(mysqlSeqFetcher, retryCount, canSendFetch);
		}

		throw new RuntimeException("fetching sequence can not support the db driver:" + dataHostConfig.getDbDriver());
	}

	private Long[] getSequenceByNaitve(FetchMySQLSequnceHandler mysqlSeqFetcher, int retryCount, boolean canSendFetch) {
		final int systemRetryCount = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlRetryCount();
		//进入waitFinish小于4次，或者可以后端获取数据
		if(retryCount <= systemRetryCount && canSendFetch) {

			this.reset();
			mysqlSeqFetcher.execute(this);
		} else if(retryCount > systemRetryCount){
			fetching.compareAndSet(true, false); //
			return null;
		}
		long start = System.currentTimeMillis();
		//直接等待
		long mysqlWaitTime = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlWaitTime();

		long end = start + mysqlWaitTime;
		while (System.currentTimeMillis() < end) {
			if(dbfinished){
				if (dbretVal == IncrSequenceMySQLHandler.errSeqResult) {
					fetching.compareAndSet(true, false); //修改
					throw new RuntimeException(
							"sequnce not found in db table ");
				}
				//进行处理 还有可能是链接错误等。
				if(dbretVal == null ){
					LOGGER.warn("can't fetch sequnce in db,sequnce :"
							+ seqName + " detail:"
							+ mysqlSeqFetcher.getLastestError(seqName) + "\n"
									+ ", and retry " + (retryCount) +" time");
					//数据库之类的连接错误，休息一下在重试。
					sleep(10);
					return getSequenceByNaitve(mysqlSeqFetcher, ++retryCount , true);
				}
				String[] items = dbretVal.split(",");

				Long curVal = Long.parseLong(items[0]);
				int span = Integer.parseInt(items[1]);
				//处理返回0，0
				if(0 == curVal) {
					LOGGER.warn("can't fetch sequnce in db,sequnce :"
							+ seqName + " detail:"
							+  " fetch return 0,0 , and retry " + (retryCount) +" time");
					//数据库之类的连接错误，休息一下在重试。
					sleep(100);
					return getSequenceByNaitve(mysqlSeqFetcher, ++retryCount, true);
				}
				fetching.compareAndSet(true, false); //修改s
				return new Long[] { curVal, curVal + span };
			} else{
				sleep(10);
			}
		}
		//等待超时 重试
		LOGGER.warn("wait sequnce in db sequnce  :"
				+ seqName + " detail:"
				+ " wait timeout " + " retry time:" + retryCount);
		return getSequenceByNaitve(mysqlSeqFetcher, ++retryCount, false);
	}


	private Long[] getSequenceValueByJDBC(int retryCount) {
		MycatConfig conf = MycatServer.getInstance().getConfig();
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(this.dataNode);
		final int systemRetryCount = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlRetryCount();
		long mysqlWaitTime = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlWaitTime();

		if (retryCount > systemRetryCount){
			this.fetching.compareAndSet(true, false);
			return null;
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("execute in datanode " + this.dataNode
					+ " for fetch sequence sql " + this.sql + " with retry count:" + retryCount);
		}

		long start = System.currentTimeMillis();
		//直接等待
		long end = start + mysqlWaitTime;
		while (System.currentTimeMillis() < end) {
			PhysicalDatasource physicalDatasource = mysqlDN.getDbPool().getSource();
			if(physicalDatasource instanceof JDBCDatasource) {
				JDBCDatasource jdbcDatasource = (JDBCDatasource) physicalDatasource;

				Connection con = null;
				PreparedStatement pstmt = null;
				ResultSet rs = null;
				try {
					con = jdbcDatasource.getDruidConnection();
					String useDB = "use " + mysqlDN.getDatabase() + ";";
					pstmt = con.prepareStatement(useDB);
					pstmt.execute();
					pstmt = con.prepareStatement(this.sql);
					rs = pstmt.executeQuery();
					String returnedValue = "";
					if (rs.next())
						returnedValue = rs.getString(1);

					if (StringUtils.isEmpty(returnedValue) || errSeqResult.equals(returnedValue)) {
						LOGGER.warn("can't fetch sequnce in db,sequnce :"
								+ this.seqName + " detail:"
								+  " fetch return -999999,null , and retry " + (retryCount) +" time");
						//数据库之类的连接错误，休息一下在重试。
						sleep(100);
						return getSequenceValueByJDBC(++retryCount);
					}
					Long curVal = Long.parseLong(returnedValue.split(",")[0]);
					Long increment = Long.parseLong(returnedValue.split(",")[1]);

					if (curVal == 0) {
						LOGGER.warn("can't fetch sequnce in db,sequnce :"
								+ this.seqName + " detail:"
								+  " fetch return 0,0 , and retry " + (retryCount) +" time");
						//数据库之类的连接错误，休息一下在重试。
						sleep(100);
						return getSequenceValueByJDBC(++retryCount);
					} else {
						this.fetching.compareAndSet(true, false);
						return new Long[]{curVal, curVal + increment};
					}
				} catch (Exception e) {
					LOGGER.warn("get sequence value err ", e);
				} finally {
					try {
						con.close();
						pstmt.close();
						rs.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		LOGGER.error("get sequence:" + this.seqName + " value failure, please check the sequence config");
		this.fetching.compareAndSet(true, false);
		return null;
	}

	private DataHostConfig getDBHostConfig() {
		MycatConfig conf = MycatServer.getInstance().getConfig();
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(this.dataNode);
		if (mysqlDN == null) {
			throw new RuntimeException("can not find the data node with name:" + this.dataNode);
		}
		PhysicalDatasource physicalDatasource = mysqlDN.getDbPool().getSource();
		return physicalDatasource.getHostConfig();
	}

	public void sleep(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			IncrSequenceMySQLHandler.LOGGER
					.warn("wait db fetch sequnce err " + e);
		}
	}
	//是否成功返回。
	public boolean isSuccessFetched() {
		return successFetched;
	}
	//下一个可用的id
	public long nextValue() {
		if (successFetched == false) {
			throw new RuntimeException(
					"sequnce fetched failed  from db ");
		}
		return curVal.incrementAndGet();
	}
}
