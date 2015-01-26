package org.opencloudb.interceptor.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.server.parser.ServerParse;
import java.io.File;

public class StatisticsSqlInterceptor implements SQLInterceptor{
	private final static Logger logger = Logger.getLogger(StatisticsSqlInterceptor.class);

	private static Map<String,Integer> typeMap = new HashMap<String,Integer>();
	static{
		typeMap.put("SELECT", 7);
		typeMap.put("UPDATE", 11);
		typeMap.put("INSERT", 4);
		typeMap.put("DELETE", 3);
	}

	private static int parseType(String type){
		return typeMap.get(type);
	}

	/**
     * 方法追加文件：使用FileWriter
     */
	private synchronized static void appendFile(String fileName, String content) {

		Calendar calendar = Calendar.getInstance();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String dayFile = dateFormat.format(calendar.getTime());

        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
        	String []title = fileName.split("\\.");
        	if(title.length==2){
        		fileName = title[0]+dayFile+"."+title[1];
        	}
        	File file = new File(fileName);
        	if(!file.exists()){
        	   if (new File(file.getParent()).mkdirs()) {
            	   file.createNewFile();
        	   }
        	}
            FileWriter writer = new FileWriter(file, true);
            content =  content.replaceAll("[\\t\\n\\r]", "") + System.getProperty("line.separator");
            writer.write(content);

            writer.close();
        } catch (IOException e) {
        	logger.error("appendFile error:"+e);
        }
    }

	/**
	 * interceptSQL ,
	 * 	type :insert,delete,update,select
	 *  exectime:xxx ms
	 *  log content : select:select 1 from table,exectime:100ms,shared:1
	 * etc
	 */
	@Override
	public String interceptSQL(String sql, int sqlType) {
		logger.debug("sql interceptSQL:");
		sql = DefaultSqlInterceptor.processEscape(sql);

		final int sqltype = sqlType ;
		final String sqls = sql;
		MycatServer.getInstance().getBusinessExecutor().execute(new Runnable() {
			public void run() {
				try {
					SystemConfig sysconfig = MycatServer.getInstance().getConfig().getSystem();
					String sqlInterceptorType = sysconfig.getSqlInterceptorType();
					String sqlInterceptorFile = sysconfig.getSqlInterceptorFile();

					String []sqlInterceptorTypes = sqlInterceptorType.split(",");
					for(String type : sqlInterceptorTypes ){
			           if(StatisticsSqlInterceptor.parseType(type.toUpperCase())==sqltype){
			        	   if(sqltype == ServerParse.SELECT){
				       			StatisticsSqlInterceptor.appendFile(sqlInterceptorFile,"SELECT:"+sqls+"");

				       	   }else if(sqltype == ServerParse.UPDATE){
				       			StatisticsSqlInterceptor.appendFile(sqlInterceptorFile,"UPDATE:"+sqls);

				       	   }else if(sqltype == ServerParse.INSERT){
				       			StatisticsSqlInterceptor.appendFile(sqlInterceptorFile,"INSERT:"+sqls);

				       	   }else if(sqltype == ServerParse.DELETE){
				       			StatisticsSqlInterceptor.appendFile(sqlInterceptorFile,"DELETE:"+sqls);

				       	   }else{

				           }
			           }
					}

				} catch (Exception e) {
					 logger.error("interceptSQL error:"+e.getMessage());
				}
			}
		});


		return sql;
	}

}
