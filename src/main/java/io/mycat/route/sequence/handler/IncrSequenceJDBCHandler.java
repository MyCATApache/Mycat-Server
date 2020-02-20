package io.mycat.route.sequence.handler;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.jdbc.JDBCDatasource;
import io.mycat.config.MycatConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.route.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Thread.sleep;

public class IncrSequenceJDBCHandler implements SequenceHandler{

    protected static final Logger LOGGER = LoggerFactory
            .getLogger(IncrSequenceJDBCHandler.class);

    private static final String SEQUENCE_DB_PROPS = "sequence_db_conf.properties";
    protected static final String errSeqResult = "-999999,null";

    private ConcurrentHashMap<String, SequenceVal> seqValueMap = new ConcurrentHashMap<String, SequenceVal>();

    private static class IncrSequenceJDBCHandlerGenerator {
        private static IncrSequenceJDBCHandler instance = new IncrSequenceJDBCHandler();
    }

    public static IncrSequenceJDBCHandler getInstance() {
        return IncrSequenceJDBCHandlerGenerator.instance;
    }

    public IncrSequenceJDBCHandler() {
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS);
        removeDesertedSequenceVals(props);
        putNewSequenceVals(props);
    }

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

    private long getSeqValueFromDB(SequenceVal seqVal) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("get next segement of sequence from db for sequnce:"
                    + seqVal.seqName + " curVal " + seqVal.curVal);
        }
        //设置正在获取
        boolean isLock = seqVal.fetching.compareAndSet(false, true);
        if(isLock) {
            //判断当前的是否有效。
            if(seqVal.successFetched) {
                Long nexVal = seqVal.nextValue();
                if (seqVal.isNexValValid(nexVal)) {
                    seqVal.fetching.compareAndSet(true, false);
                    return nexVal;
                }
            }
            //重置之前的值为空
            seqVal.reset();
            //发起请求sql 等待到返回  或者进行
            Long[] values = getSequenceValueBySeqName(seqVal, 1); //只有一个线程可以进 并且有重试机制。
            if (values == null) {
                throw new RuntimeException("can't fetch sequnce in db,sequnce :"
                        + seqVal.seqName);
            } else {
                seqVal.setCurValue(values[0]);
                seqVal.maxSegValue = values[1];
                seqVal.successFetched = true; //设置successFetched
                return values[0];
            }
        } else {
            long count = 0 ;
            //正在获取 ，或者还未返回
            while(seqVal.fetching.get() || !seqVal.successFetched){
                try {
                    sleep(10);
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

    private Long[] getSequenceValueBySeqName(SequenceVal seqVal, int retryCount) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        PhysicalDBNode mysqlDN = conf.getDataNodes().get(seqVal.dataNode);
        final int systemRetryCount = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlRetryCount();
        long mysqlWaitTime = MycatServer.getInstance().getConfig().getSystem().getSequnceMySqlWaitTime();

        if (retryCount > systemRetryCount){
            seqVal.fetching.compareAndSet(true, false);
            return null;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("execute in datanode " + seqVal.dataNode
                    + " for fetch sequence sql " + seqVal.sql + " with retry count:" + retryCount);
        }

        long start = System.currentTimeMillis();
        //直接等待
        long end = start + mysqlWaitTime;
        while (System.currentTimeMillis() < end) {
            PhysicalDatasource physicalDatasource = mysqlDN.getDbPool().getSource();
            if(physicalDatasource instanceof JDBCDatasource) {
                JDBCDatasource jdbcDatasource = (JDBCDatasource) physicalDatasource;

                Connection con = null;
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                try {
                    con = jdbcDatasource.getDruidConnection();
                    String useDB = "use " + mysqlDN.getDatabase() + ";";
                    pstmt = con.prepareStatement(useDB);
                    pstmt.execute();
                    pstmt = con.prepareStatement(seqVal.sql);
                    rs = pstmt.executeQuery();
                    String returnedValue = "";
                    if (rs.next())
                        returnedValue = rs.getString(1);

                    if (StringUtils.isEmpty(returnedValue) || errSeqResult.equals(returnedValue)) {
                        LOGGER.warn("can't fetch sequnce in db,sequnce :"
                                + seqVal.seqName + " detail:"
                                +  " fetch return -999999,null , and retry " + (retryCount) +" time");
                        //数据库之类的连接错误，休息一下在重试。
                        sleep(100);
                        return getSequenceValueBySeqName(seqVal, ++retryCount);
                    }
                    Long curVal = Long.parseLong(returnedValue.split(",")[0]);
                    Long increment = Long.parseLong(returnedValue.split(",")[1]);

                    if (curVal == 0) {
                        LOGGER.warn("can't fetch sequnce in db,sequnce :"
                                + seqVal.seqName + " detail:"
                                +  " fetch return 0,0 , and retry " + (retryCount) +" time");
                        //数据库之类的连接错误，休息一下在重试。
                        sleep(100);
                        return getSequenceValueBySeqName(seqVal, ++retryCount);
                    } else {
                        seqVal.fetching.compareAndSet(true, false);
                        return new Long[]{curVal, curVal + increment};
                    }
                } catch (Exception e) {
                    LOGGER.warn("get sequence value err ", e);
                } finally {
                    try {
                        con.close();
                        pstmt.close();
                        rs.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        LOGGER.error("get sequence:" + seqVal.seqName + " value failure, please check the sequence config");
        seqVal.fetching.compareAndSet(true, false);
        return null;
    }

}
