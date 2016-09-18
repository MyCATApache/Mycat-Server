package io.mycat.route;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.parser.druid.DruidSequenceHandler;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

public class MyCATSequnceProcessor {
	private static final Logger LOGGER = LoggerFactory.getLogger(MyCATSequnceProcessor.class);
	private LinkedBlockingQueue<SessionSQLPair> seqSQLQueue = new LinkedBlockingQueue<SessionSQLPair>();
	private volatile boolean running=true;
	
	public MyCATSequnceProcessor() {
		new ExecuteThread().start();
	}

	public void addNewSql(SessionSQLPair pair) {
		seqSQLQueue.add(pair);
	}

	private void outRawData(ServerConnection sc,String value) {
		byte packetId = 0;
		int fieldCount = 1;
		ByteBuffer byteBuf = sc.allocate();
		ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
		headerPkg.fieldCount = fieldCount;
		headerPkg.packetId = ++packetId;

		byteBuf = headerPkg.write(byteBuf, sc, true);
		FieldPacket fieldPkg = new FieldPacket();
		fieldPkg.packetId = ++packetId;
		fieldPkg.name = StringUtil.encode("SEQUNCE", sc.getCharset());
		byteBuf = fieldPkg.write(byteBuf, sc, true);

		EOFPacket eofPckg = new EOFPacket();
		eofPckg.packetId = ++packetId;
		byteBuf = eofPckg.write(byteBuf, sc, true);

		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.packetId = ++packetId;
		rowDataPkg.add(StringUtil.encode(value, sc.getCharset()));
		byteBuf = rowDataPkg.write(byteBuf, sc, true);
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		byteBuf = lastEof.write(byteBuf, sc, true);

		// write buffer
		sc.write(byteBuf);
	}

	private void executeSeq(SessionSQLPair pair) {
		try {
			/*// @micmiu 扩展NodeToString实现自定义全局序列号
			NodeToString strHandler = new ExtNodeToString4SEQ(MycatServer
					.getInstance().getConfig().getSystem()
					.getSequnceHandlerType());
			// 如果存在sequence 转化sequence为实际数值
			String charset = pair.session.getSource().getCharset();
			QueryTreeNode ast = SQLParserDelegate.parse(pair.sql,
					charset == null ? "utf-8" : charset);
			String sql = strHandler.toString(ast);
			if (sql.toUpperCase().startsWith("SELECT")) {
				String value=sql.substring("SELECT".length()).trim();
				outRawData(pair.session.getSource(),value);
				return;
			}*/
			
			//使用Druid解析器实现sequence处理  @兵临城下
			DruidSequenceHandler sequenceHandler = new DruidSequenceHandler(MycatServer
					.getInstance().getConfig().getSystem().getSequnceHandlerType());
			
			String charset = pair.session.getSource().getCharset();
			String executeSql = sequenceHandler.getExecuteSql(pair.sql,charset == null ? "utf-8":charset);
			
			pair.session.getSource().routeEndExecuteSQL(executeSql, pair.type,pair.schema);
		} catch (Exception e) {
			LOGGER.error("MyCATSequenceProcessor.executeSeq(SesionSQLPair)",e);
			pair.session.getSource().writeErrMessage(ErrorCode.ER_YES,"mycat sequnce err." + e);
			return;
		}
	}
	
	public void shutdown(){
		running=false;
	}
	
	class ExecuteThread extends Thread {
		
		public ExecuteThread() {
			setDaemon(true); // 设置为后台线程,防止throw RuntimeExecption进程仍然存在的问题
		}
		
		public void run() {
			while (running) {
				try {
					SessionSQLPair pair=seqSQLQueue.poll(100,TimeUnit.MILLISECONDS);
					if(pair!=null){
						executeSeq(pair);
					}
				} catch (Exception e) {
					LOGGER.warn("MyCATSequenceProcessor$ExecutorThread",e);
				}
			}
		}
	}
}
