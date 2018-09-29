package io.mycat.route.sequence.handler;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.MycatConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.util.PropertiesUtil;
import io.mycat.server.parser.ServerParse;

public class IncrSequenceMySQLHandler implements SequenceHandler {

	protected static final Logger LOGGER = LoggerFactory
			.getLogger(IncrSequenceMySQLHandler.class);

	private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
	protected static final String errSeqResult = "-999999,null";
	protected static final String errlockSeqResult = "0,0"; //数据库lock失败 返回0, 0

	protected static Map<String, String> latestErrors = new ConcurrentHashMap<String, String>();
	private final FetchMySQLSequnceHandler mysqlSeqFetcher = new FetchMySQLSequnceHandler();

	private static class IncrSequenceMySQLHandlerHolder {
		private static final IncrSequenceMySQLHandler instance = new IncrSequenceMySQLHandler();
	}

	public static IncrSequenceMySQLHandler getInstance() {
		return IncrSequenceMySQLHandlerHolder.instance;
	}

	public IncrSequenceMySQLHandler() {

		load();
	}
	//加载配置文件
	public void load() {
		// load sequnce properties
		Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS);
		removeDesertedSequenceVals(props);
		putNewSequenceVals(props);
	}

	//移除旧的配置
	private void removeDesertedSequenceVals(Properties props) {
		Iterator<Map.Entry<String, SequenceVal>> i = seqValueMap.entrySet()
				.iterator();
		while (i.hasNext()) {
			Map.Entry<String, SequenceVal> entry = i.next();
			if (!props.containsKey(entry.getKey())) {
				i.remove();
			}
		}
	}

	private void putNewSequenceVals(Properties props) {
		for (Map.Entry<Object, Object> entry : props.entrySet()) {
			String seqName = (String) entry.getKey();
			String dataNode = (String) entry.getValue();
			if (!seqValueMap.containsKey(seqName)) {
				seqValueMap.put(seqName, new SequenceVal(seqName, dataNode));
			} else {
				seqValueMap.get(seqName).dataNode = dataNode;
			}
		}
	}

	/**
	 * save sequnce -> curval
	 */
	private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<String, SequenceVal>();

	@Override
	public long nextId(String seqName) {
		SequenceVal seqVal = seqValueMap.get(seqName);
		if (seqVal == null) {
			throw new ConfigException("can't find definition for sequence :"
					+ seqName);
		}
		if (!seqVal.isSuccessFetched()) {
			//从数据库获取
			return getSeqValueFromDB(seqVal);
		} else {
			//已经设置 获取下一个有效id
			return getNextValidSeqVal(seqVal);
		}

	}
	//获取有效的sequence
	private Long getNextValidSeqVal(SequenceVal seqVal) {
		Long nexVal = seqVal.nextValue();
		//当前id有效返回 无效则从数据库获取
		if (seqVal.isNexValValid(nexVal)) {
			return nexVal;
		} else {
//			seqVal.fetching.compareAndSet(true, false);
			return getSeqValueFromDB(seqVal);
		}
	}

	//在这边用锁。
	/*
	 * 1.尝试获取fetching锁， 
	 *    成功： 
	 *       再次判断下一个值是否有效，有效直接返回
	 *       否则 调用waitFinish请求后端的请求
	 *     失败：
	 *        进行等待，等待一定次数 尝试从数据库获取
	 *             别的后端请求已经成功获取，调用getNextValidSeqVal获取下一个可用值
	 *  
	 * 
	 * */
	private long getSeqValueFromDB(SequenceVal seqVal) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("get next segement of sequence from db for sequnce:"
					+ seqVal.seqName + " curVal " + seqVal.curVal);
		}
		//设置正在获取
		boolean isLock = seqVal.fetching.compareAndSet(false, true);
//		//唯一的一个去获取数据。
//		if (isLock) {
//			seqVal.dbretVal = null;
//			seqVal.dbfinished = false;
//			seqVal.newValueSetted.set(false);
//			seqVal.successFetched = false; //添加代码
//			mysqlSeqFetcher.execute(seqVal);
//			isLock = true;			
//		}		
		if(isLock) {
			//判断当前的是否有效。
			if(seqVal.successFetched == true) {
				Long nexVal = seqVal.nextValue();
				if (seqVal.isNexValValid(nexVal)) {
					seqVal.fetching.compareAndSet(true, false);
					return nexVal;
				}
			}
						
			//发起请求sql 等待到返回  或者进行
			Long[] values = seqVal.waitFinish( mysqlSeqFetcher, 1, true); //只有一个线程可以进 并且有重试机制。
			if (values == null) {
				
				throw new RuntimeException("can't fetch sequnce in db,sequnce :"
						+ seqVal.seqName + " detail:"
						+ mysqlSeqFetcher.getLastestError(seqVal.seqName));
			} else {
					seqVal.setCurValue(values[0]); 
					seqVal.maxSegValue = values[1];
					seqVal.successFetched = true; //设置successFetched
					return values[0];
			
			}
		} else {
			long count = 0 ;
			//正在获取 ，或者还未返回
			while(seqVal.fetching.get() || seqVal.successFetched == false){
				try {										
					Thread.sleep(10);
					if(++count > 10000L) {
						return this.getSeqValueFromDB(seqVal);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					return this.getSeqValueFromDB(seqVal);
				}
			}
			return this.getNextValidSeqVal(seqVal);
		}	

	}
}

class FetchMySQLSequnceHandler implements ResponseHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(FetchMySQLSequnceHandler.class);

	public void execute(SequenceVal seqVal) {
		MycatConfig conf = MycatServer.getInstance().getConfig();
		PhysicalDBNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
		try {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("execute in datanode " + seqVal.dataNode
						+ " for fetch sequnce sql " + seqVal.sql);
			}
			// 修正获取seq的逻辑，在读写分离的情况下只能走写节点。修改Select模式为Update模式。
			mysqlDN.getConnection(mysqlDN.getDatabase(), true,
					new RouteResultsetNode(seqVal.dataNode, ServerParse.UPDATE,
							seqVal.sql), this, seqVal);
		} catch (Exception e) {
			seqVal.dbfinished = true;

			LOGGER.warn("get connection err " + e);
		}

	}

	public String getLastestError(String seqName) {
		return IncrSequenceMySQLHandler.latestErrors.get(seqName);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {

		conn.setResponseHandler(this);
		try {
			//发起sql请求
			SequenceVal sequenceVal = ((SequenceVal) conn.getAttachment()) ;
			conn.query(sequenceVal.sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}
	//连接错误
	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		((SequenceVal) conn.getAttachment()).dbfinished = true;
		LOGGER.warn("connectionError " + e);

	}
	//返回错误结果处理
	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		SequenceVal seqVal = ((SequenceVal) conn.getAttachment());
//		seqVal.dbfinished = true;
		seqVal.setDbfinished();
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		String errMsg = new String(err.message);
		LOGGER.warn("errorResponse " + err.errno + " " + errMsg);
		IncrSequenceMySQLHandler.latestErrors.put(seqVal.seqName, errMsg);
		conn.release();

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			((SequenceVal) conn.getAttachment()).setDbfinished();
//			((SequenceVal) conn.getAttachment()).dbfinished = true;
			conn.release();
		}

	}
	//获取一行的数据 
	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		RowDataPacket rowDataPkg = new RowDataPacket(1);
		rowDataPkg.read(row);
		byte[] columnData = rowDataPkg.fieldValues.get(0);
		String columnVal = new String(columnData);
		SequenceVal seqVal = (SequenceVal) conn.getAttachment();
		if (IncrSequenceMySQLHandler.errSeqResult.equals(columnVal)) {
			seqVal.dbretVal = IncrSequenceMySQLHandler.errSeqResult;
			LOGGER.warn(" sequnce sql returned err value ,sequence:"
					+ seqVal.seqName + " " + columnVal + " sql:" + seqVal.sql);
		}else {
			seqVal.dbretVal = columnVal;
		}
	}
	//结果集合接受完毕
	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		SequenceVal sequenceVal = ((SequenceVal) conn.getAttachment());
		conn.release();
//		sequenceVal.dbfinished = true;
		sequenceVal.setDbfinished();
	}
	//错误返回异常处理
	private void executeException(BackendConnection c, Throwable e) {
		SequenceVal seqVal = ((SequenceVal) c.getAttachment());
//		seqVal.dbfinished = true;
		seqVal.setDbfinished();
		String errMgs=e.toString();
		IncrSequenceMySQLHandler.latestErrors.put(seqVal.seqName, errMgs);
		LOGGER.warn("executeException   " + errMgs);
		c.close("exception:" +errMgs);

	}

	@Override
	public void writeQueueAvailable() {

	}
	//连接关闭异常处理
	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		//((SequenceVal) conn.getAttachment()).dbfinished = true;
		((SequenceVal) conn.getAttachment()).setDbfinished();
		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}

}

class SequenceVal {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(FetchMySQLSequnceHandler.class);
//	public AtomicBoolean newValueSetted = new AtomicBoolean(false);
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
//		newValueSetted.set(false);
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
	public Long[] waitFinish(FetchMySQLSequnceHandler mysqlSeqFetcher, int retryCount, boolean canSendFetch) {
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
					throw new java.lang.RuntimeException(
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
					return waitFinish(mysqlSeqFetcher, ++retryCount , true);
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
					return waitFinish(mysqlSeqFetcher, ++retryCount, true);
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
		return waitFinish(mysqlSeqFetcher, ++retryCount, false);
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
			throw new java.lang.RuntimeException(
					"sequnce fetched failed  from db ");
		}
		return curVal.incrementAndGet();
	}
}
