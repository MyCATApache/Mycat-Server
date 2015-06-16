package org.opencloudb.parser.druid;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.sequence.handler.IncrSequenceTimeHandler;
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

	/** 获取MYCAT SEQ的匹配语句 */
	private final static String MATCHED_FEATURE = "NEXT VALUE FOR MYCATSEQ_";

    private final static   Pattern pattern = Pattern.compile("(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+", Pattern.CASE_INSENSITIVE);
	
	public DruidSequenceHandler(int seqHandlerType) {
		switch(seqHandlerType){
		case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
			sequenceHandler = IncrSequenceMySQLHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
			sequenceHandler = IncrSequencePropHandler.getInstance();
			break;
		case SystemConfig.SEQUENCEHANDLER_LOCAL_TIME:
			sequenceHandler = IncrSequenceTimeHandler.getInstance();
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
             //sql不能转大写，因为sql可能是insert语句会把values也给转换了
			// 获取表名。
            Matcher matcher = pattern.matcher(sql);
              if(matcher.find())
              {
                  String tableName = matcher.group(2);
                  long value = sequenceHandler.nextId(tableName.toUpperCase());

                  // 将MATCHED_FEATURE+表名替换成序列号。
                  executeSql = sql.replace(matcher.group(1), " "+value);
              }

		}
		return executeSql;
	}


    //just for test
	public String getTableName(String sql) {
        Matcher matcher = pattern.matcher(sql);
        if(matcher.find())
        {
          return  matcher.group(2);
        }
        return null;
	}



}
