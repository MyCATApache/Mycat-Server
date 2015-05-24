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
			// 转换成大写。
			sql = new String(sql.getBytes(), charset).toUpperCase();
            if(sql.startsWith("SELECT "))
            {
            //   return  sql;
            }
			// 获取表名。
            String p="(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+";
            Pattern pattern = Pattern.compile(p,Pattern.CASE_INSENSITIVE);
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
	
	public String getTableName(String sql) {
		int beginIndex = sql.indexOf(MATCHED_FEATURE);
		if(beginIndex == -1 || beginIndex == sql.length()) {
			throw new RuntimeException(sql+" 中应包含语句 "+MATCHED_FEATURE);
		}

        String maybe = sql.substring(beginIndex + MATCHED_FEATURE.length());
        int dIndex = maybe.indexOf(",");
        if(dIndex!=-1)
        {
            return maybe.substring(0, dIndex).trim()  ;
        }
        int kIndex = maybe.indexOf(")");
        if(kIndex!=-1)
        {
            return maybe.substring(0, kIndex).trim()  ;
        }
        return maybe;
	}

	/**
	 * TODO 此部分未明了其含义，如有问题，请联系BEN
	 * 
	 * @param orgSql
	 * @param tableName
	 * @param sequnce
	 * @return
	 */
	private String replaceSql(String orgSql,String tableName,long sequnce){
		if(orgSql.indexOf(MATCHED_FEATURE)==-1){
			throw new java.lang.IllegalArgumentException("Invalid sequnce Sql , must need "+MATCHED_FEATURE);
		}
		String squenceStr = MATCHED_FEATURE;
		String repanceStr = MATCHED_FEATURE+ tableName;

		int startIndex = orgSql.indexOf(squenceStr) + squenceStr.length();
		int endIndex = startIndex + tableName.length();
		orgSql = orgSql.substring(0, startIndex)
			   + orgSql.substring(startIndex, endIndex).toUpperCase()
			   + orgSql.substring(endIndex, orgSql.length()) ;
		orgSql = orgSql.replace(repanceStr, sequnce+"");

		return orgSql;
	}

}
