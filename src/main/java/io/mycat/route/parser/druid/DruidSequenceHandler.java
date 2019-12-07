package io.mycat.route.parser.druid;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.mycat.MycatServer;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.SessionSQLPair;
import io.mycat.route.sequence.handler.*;
import io.mycat.util.TimeUtil;

/**
 * 使用Druid解析器实现对Sequence处理
 *
 * @author 兵临城下
 * @date 2015/03/13
 */
public class DruidSequenceHandler {
    private final SequenceHandler sequenceHandler;

    /**
     * 分段锁
     */
    private final static Map<String,ReentrantLock> segmentLock = new ConcurrentHashMap<>();

    /**
     * 获取MYCAT SEQ的匹配语句
     */
    private final static String MATCHED_FEATURE = "NEXT VALUE FOR MYCATSEQ_";

    private final  Pattern pattern;

    public DruidSequenceHandler(int seqHandlerType,String sequnceHandlerPattern) {
      this.pattern =  Pattern.compile(sequnceHandlerPattern, Pattern.CASE_INSENSITIVE);
        switch (seqHandlerType) {
            case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
                sequenceHandler = IncrSequenceMySQLHandler.getInstance();
                break;
            case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
                sequenceHandler = IncrSequencePropHandler.getInstance();
                break;
            case SystemConfig.SEQUENCEHANDLER_LOCAL_TIME:
                sequenceHandler = IncrSequenceTimeHandler.getInstance();
                break;
            case SystemConfig.SEQUENCEHANDLER_ZK_DISTRIBUTED:
                sequenceHandler = DistributedSequenceHandler.getInstance(MycatServer.getInstance().getConfig().getSystem());
                break;
            case SystemConfig.SEQUENCEHANDLER_ZK_GLOBAL_INCREMENT:
                sequenceHandler = IncrSequenceZKHandler.getInstance();
                break;
            case SystemConfig.SEQUENCEHANDLER_DEF_GLOBAL_INCREMENT:
                try {
                    String sequenceHanlderClass = MycatServer.getInstance().getConfig().getSystem().getSequenceHanlderClass();
                    Class<?> aClass = Class.forName(sequenceHanlderClass);
                    Constructor constructor=aClass.getConstructor();
                    sequenceHandler  =(SequenceHandler) constructor.newInstance();
                }catch (Exception e){
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                break;
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequnce handler type " + seqHandlerType);
        }
    }

    /**
     * 根据原sql获取可执行的sql
     *
     * @param sql
     * @return
     * @throws UnsupportedEncodingException
     */
    public String getExecuteSql(SessionSQLPair pair, String charset) throws UnsupportedEncodingException,InterruptedException {
    	String executeSql = pair.sql;
        if (null != pair.sql && !"".equals(pair.sql)) {
            Matcher matcher = pattern.matcher(executeSql);
            if(matcher.find()){
            	String tableName = matcher.group(2);
                ReentrantLock lock = getSegLock(tableName);
				lock.lock();
				try {
                	matcher = pattern.matcher(executeSql);
                	while(matcher.find()){
                		long value = sequenceHandler.nextId(tableName.toUpperCase());
                        executeSql = executeSql.replaceFirst(matcher.group(1), Long.toString(value));
                        pair.session.getSource().setLastWriteTime(TimeUtil.currentTimeMillis());
                    }
				} finally {
					lock.unlock();
				}
            }
        }
        return executeSql;
    }

    /*
     * 获取分段锁
     * @param name
     * @return
     */
    private ReentrantLock getSegLock(String name){
    	ReentrantLock lock = segmentLock.get(name);
    	if(lock==null){
    		synchronized (segmentLock) {
    			lock = segmentLock.get(name);
				if(lock==null){
					lock = new ReentrantLock();
					segmentLock.put(name, lock);
				}
			}
    	}
    	return lock;
    }


    //just for test
    public String getTableName(String sql) {
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }


}
