package org.opencloudb.sequence.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.parser.ServerParse;

public class IncrSequenceMySQLHandler implements SequenceHandler {

	protected static final Logger LOGGER = Logger
			.getLogger(IncrSequenceMySQLHandler.class);

	private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
	protected static final String errSeqResult = "-999999999,null";
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

	public void load() {
		// load sequnce properties
		Properties props = loadProps(SEQUENCE_DB_PROPS);
		removeDesertedSequenceVals(props);
		putNewSequenceVals(props);
	}

	private Properties loadProps(String propsFile) {

		Properties props = new Properties();
		InputStream inp = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream(propsFile);

		if (inp == null) {
			throw new java.lang.RuntimeException(
					"db sequnce properties not found " + propsFile);

		}
		try {
			props.load(inp);
		} catch (IOException e) {
			throw new java.lang.RuntimeException(e);
		}

		return props;
	}

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
			return getSeqValueFromDB(seqVal);
		} else {
			return getNextValidSeqVal(seqVal);
		}

	}

	private Long getNextValidSeqVal(SequenceVal seqVal) {
		Long nexVal = seqVal.nextValue();
		if (seqVal.isNexValValid(nexVal)) {
			return nexVal;
		} else {
			seqVal.fetching.compareAndSet(true, false);
			return getSeqValueFromDB(seqVal);
		}
	}

	private long getSeqValueFromDB(SequenceVal seqVal) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("get next segement of sequence from db for sequnce:"
					+ seqVal.seqName + " curVal " + seqVal.curVal);
		}
		if (seqVal.fetching.compareAndSet(false, true)) {
			seqVal.dbretVal = null;
			seqVal.dbfinished = false;
			seqVal.newValueSetted.set(false);
			mysqlSeqFetcher.execute(seqVal);
		}
		Long[] values = seqVal.waitFinish();
		if (values == null) {

			throw new RuntimeException("can't fetch sequnce in db,sequnce :"
					+ seqVal.seqName + " detail:"
					+ mysqlSeqFetcher.getLastestError(seqVal.seqName));
		} else {
			if (seqVal.newValueSetted.compareAndSet(false, true)) {
				seqVal.setCurValue(values[0]);
				seqVal.maxSegValue = values[1];
				return values[0];
			} else {
				return seqVal.nextValue();
			}

		}

	}
}

class FetchMySQLSequnceHandler implements ResponseHandler {
	private static final Logger LOGGER = Logger
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
			conn.query(((SequenceVal) conn.getAttachment()).sql);
		} catch (Exception e) {
			executeException(conn, e);
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		((SequenceVal) conn.getAttachment()).dbfinished = true;
		LOGGER.warn("connectionError " + e);

	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		SequenceVal seqVal = ((SequenceVal) conn.getAttachment());
		seqVal.dbfinished = true;

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
			((SequenceVal) conn.getAttachment()).dbfinished = true;
			conn.release();
		}

	}

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
		} else {
			seqVal.dbretVal = columnVal;
		}
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		((SequenceVal) conn.getAttachment()).dbfinished = true;
		conn.release();
	}

	private void executeException(BackendConnection c, Throwable e) {
		SequenceVal seqVal = ((SequenceVal) c.getAttachment());
		seqVal.dbfinished = true;
		String errMgs=e.toString();
		IncrSequenceMySQLHandler.latestErrors.put(seqVal.seqName, errMgs);
		LOGGER.warn("executeException   " + errMgs);
		c.close("exception:" +errMgs);

	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

		LOGGER.warn("connection closed " + conn + " reason:" + reason);
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}

}

class SequenceVal {
	public AtomicBoolean newValueSetted = new AtomicBoolean(false);
	public AtomicLong curVal = new AtomicLong(0);
	public volatile String dbretVal = null;
	public volatile boolean dbfinished;
	public AtomicBoolean fetching = new AtomicBoolean(false);
	public volatile long maxSegValue;
	public volatile boolean successFetched;
	public volatile String dataNode;
	public final String seqName;
	public final String sql;

	public SequenceVal(String seqName, String dataNode) {
		this.seqName = seqName;
		this.dataNode = dataNode;
		sql = "SELECT mycat_seq_nextval('" + seqName + "')";
	}

	public boolean isNexValValid(Long nexVal) {
		if (nexVal < this.maxSegValue) {
			return true;
		} else {
			return false;
		}
	}

	FetchMySQLSequnceHandler seqHandler;

	public void setCurValue(long newValue) {
		curVal.set(newValue);
		successFetched = true;
	}

	public Long[] waitFinish() {
		long start = System.currentTimeMillis();
		long end = start + 10 * 1000;
		while (System.currentTimeMillis() < end) {
			if (dbretVal == IncrSequenceMySQLHandler.errSeqResult) {
				throw new java.lang.RuntimeException(
						"sequnce not found in db table ");
			} else if (dbretVal != null) {
				String[] items = dbretVal.split(",");
				Long curVal = Long.valueOf(items[0]);
				int span = Integer.valueOf(items[1]);
				return new Long[] { curVal, curVal + span };
			} else {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					IncrSequenceMySQLHandler.LOGGER
							.warn("wait db fetch sequnce err " + e);
				}
			}
		}
		return null;
	}

	public boolean isSuccessFetched() {
		return successFetched;
	}

	public long nextValue() {
		if (successFetched == false) {
			throw new java.lang.RuntimeException(
					"sequnce fetched failed  from db ");
		}
		return curVal.incrementAndGet();
	}
}
