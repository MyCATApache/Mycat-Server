package org.opencloudb.parser.druid;

import java.io.UnsupportedEncodingException;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.sequence.handler.IncrSequenceMySQLHandler;
import org.opencloudb.sequence.handler.IncrSequencePropHandler;
import org.opencloudb.sequence.handler.SequenceHandler;
import org.opencloudb.util.StringUtil;

/**
 * 使用Druid解析器实现对Sequence处理
 * @author 兵临城下
 * @date 2015/03/13
 */
public class DruidSequenceHandler {
	private final SequenceHandler sequenceHandler;

	public DruidSequenceHandler(int seqHandlerType) {
		switch(seqHandlerType){
		case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
			sequenceHandler = IncrSequenceMySQLHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
			sequenceHandler = IncrSequencePropHandler.getInstance();
			break;
		default:
			throw new java.lang.IllegalArgumentException("Invalid sequnce handler type "+seqHandlerType);
		}
	}
	
	/**
	 * 根据原sql获取可执行的sql
	 * @param sql
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	public String getExecuteSql(String sql,String charset) throws UnsupportedEncodingException{
		String executeSql = null;
		if (null!=sql && !"".equals(sql)) {
			sql = new String(sql.getBytes(), charset);
			String tableName = StringUtil.getTableName(sql).toUpperCase();
			long value = sequenceHandler.nextId(tableName.toUpperCase());
			String replaceStr = "next value for MYCATSEQ_"+tableName;
			executeSql = sql.replace(replaceStr, value+"");
		}
		return executeSql;
	}

}
