package io.mycat.server.interceptor.impl;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.interceptor.SQLInterceptor;
import io.mycat.server.parser.ServerParse;

import java.io.File;

public class StatisticsSqlInterceptor implements SQLInterceptor {
    
private final class StatisticsSqlRunner implements Runnable {
        
        private int    sqltype = 0;
        private String sqls    = "";
        
        public StatisticsSqlRunner(int sqltype, String sqls) {
            this.sqltype = sqltype;
            this.sqls = sqls;
        }
        
        public void run() {
            try {
                SystemConfig sysconfig = MycatServer.getInstance().getConfig().getSystem();
                String sqlInterceptorType = sysconfig.getSqlInterceptorType();
                String sqlInterceptorFile = sysconfig.getSqlInterceptorFile();
                
                String[] sqlInterceptorTypes = sqlInterceptorType.split(",");
                for (String type : sqlInterceptorTypes) {
                    if (StatisticsSqlInterceptor.parseType(type.toUpperCase()) == sqltype) {
                        switch (sqltype) {
                            case ServerParse.SELECT:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "SELECT:"
                                    + sqls + "");
                                break;
                            case ServerParse.UPDATE:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "UPDATE:"
                                    + sqls);
                                break;
                            case ServerParse.INSERT:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "INSERT:"
                                    + sqls);
                                break;
                            case ServerParse.DELETE:
                                StatisticsSqlInterceptor.appendFile(sqlInterceptorFile, "DELETE:"
                                    + sqls);
                                break;
                            default:
                                break;
                        }
                    }
                }
                
            } catch (Exception e) {
                LOGGER.error("interceptSQL error:" + e.getMessage(),e);
            }
        }
    }
    
    private static final Logger         LOGGER  = LoggerFactory.getLogger(StatisticsSqlInterceptor.class);
    
    private static Map<String, Integer> typeMap = new HashMap<String, Integer>();
    static {
        typeMap.put("SELECT", 7);
        typeMap.put("UPDATE", 11);
        typeMap.put("INSERT", 4);
        typeMap.put("DELETE", 3);
    }
    
    public static int parseType(String type) {
        return typeMap.get(type);
    }
    
    /**
     * 方法追加文件：使用FileWriter
     */
    private static synchronized void appendFile(String fileName, String content) {
        
        Calendar calendar = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dayFile = dateFormat.format(calendar.getTime());
        FileWriter writer = null;
        try {
            String newFileName = fileName;
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            String[] title = newFileName.split("\\.");
            if (title.length == 2) {
                newFileName = title[0] + dayFile + "." + title[1];
            }
            File file = new File(newFileName);
            if (!file.exists()) {
                file.createNewFile();
            }
            writer = new FileWriter(file, true);
            String newContent = content.replaceAll("[\\t\\n\\r]", "")
                + System.getProperty("line.separator");
            writer.write(newContent);
            
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("appendFile error:" + e.getMessage(),e);
        } finally {
            if(writer != null ){
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("close file error:" + e.getMessage(),e);
                }
            }
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
        LOGGER.debug("sql interceptSQL:");
        
        final int sqltype = sqlType;
        final String sqls = DefaultSqlInterceptor.processEscape(sql);
        MycatServer.getInstance().getBusinessExecutor()
            .execute(new StatisticsSqlRunner(sqltype, sqls));
        return sql;
    }
    
}
