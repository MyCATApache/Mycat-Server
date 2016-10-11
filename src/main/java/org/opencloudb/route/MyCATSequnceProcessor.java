package org.opencloudb.route;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.parser.druid.DruidSequenceHandler;
import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.NameableExecutor;

public class MyCATSequnceProcessor {
	private static final Logger LOGGER = Logger.getLogger(MyCATSequnceProcessor.class);
	private LinkedBlockingQueue<SessionSQLPair> seqSQLQueue = new LinkedBlockingQueue<SessionSQLPair>();
	private volatile boolean running=true;
	private NameableExecutor sequenceExecutor;

	public MyCATSequnceProcessor(int processorCount) {
		sequenceExecutor = ExecutorUtil.create("sequenceExecutor", processorCount);
        for(int i=0;i<processorCount;i++){
        	sequenceExecutor.execute(new ExecuteThread());
        }
	}

	public void addNewSql(SessionSQLPair pair) {
		seqSQLQueue.add(pair);
	}
	private void executeSeq(SessionSQLPair pair) {
		try {
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
