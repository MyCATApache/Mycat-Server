package io.mycat.config.table.structure;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表结构结果处理
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:09:03 2016/5/11
 */
public class MySQLTableStructureDetector implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLTableStructureDetector.class);
    private static final String[] MYSQL_SHOW_CREATE_TABLE_COLMS = new String[]{
            "Table",
            "Create Table"};
    private static final String sqlPrefix = "show create table ";

    @Override
    public void run() {
        for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) {
            for (TableConfig table : schema.getTables().values()) {
                for (String dataNode : table.getDataNodes()) {
                    try {
                        table.getReentrantReadWriteLock().writeLock().lock();
                        ConcurrentHashMap<String, List<String>> map = new ConcurrentHashMap<>();
                        table.setDataNodeTableStructureSQLMap(map);
                    } finally {
                        table.getReentrantReadWriteLock().writeLock().unlock();
                    }
                    OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(MYSQL_SHOW_CREATE_TABLE_COLMS, new MySQLTableStructureListener(dataNode, table));
                    resultHandler.setMark("Table Structure");
                    PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(dataNode);
                    SQLJob sqlJob = new SQLJob(sqlPrefix + table.getName(), dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
                    sqlJob.run();
                }
            }
        }
    }

    private static class MySQLTableStructureListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
        private String dataNode;
        private TableConfig table;

        public MySQLTableStructureListener(String dataNode, TableConfig table) {
            this.dataNode = dataNode;
            this.table = table;
        }

        /**
         * @param result
         * @// TODO: 2016/5/11 检查表元素，来确定是哪个元素不一致，未来还有其他用
         */
        @Override
        public void onResult(SQLQueryResult<Map<String, String>> result) {
            try {
                table.getReentrantReadWriteLock().writeLock().lock();
                if (!result.isSuccess()) {
                    LOGGER.warn("Can't get table " + table.getName() + "'s config from DataNode:" + dataNode + "! Maybe the table is not initialized!");
                    return;
                }
                String currentSql = result.getResult().get(MYSQL_SHOW_CREATE_TABLE_COLMS[1]);
                Map<String, List<String>> dataNodeTableStructureSQLMap = table.getDataNodeTableStructureSQLMap();
                if (dataNodeTableStructureSQLMap.containsKey(currentSql)) {
                    List<String> dataNodeList = dataNodeTableStructureSQLMap.get(currentSql);
                    dataNodeList.add(dataNode);
                } else {
                    List<String> dataNodeList = new LinkedList<>();
                    dataNodeList.add(dataNode);
                    dataNodeTableStructureSQLMap.put(currentSql,dataNodeList);
                }
                if (dataNodeTableStructureSQLMap.size() > 1) {
                    LOGGER.warn("Table [" + table.getName() + "] structure are not consistent!");
                    LOGGER.warn("Currently detected: ");
                    for(String sql : dataNodeTableStructureSQLMap.keySet()){
                        StringBuilder stringBuilder = new StringBuilder();
                        for(String dn : dataNodeTableStructureSQLMap.get(sql)){
                            stringBuilder.append("DataNode:[").append(dn).append("]");
                        }
                        stringBuilder.append(":").append(sql);
                        LOGGER.warn(stringBuilder.toString());
                    }
                }
            } finally {
                table.getReentrantReadWriteLock().writeLock().unlock();
            }
        }
    }

//    public static void main(String[] args) {
//        System.out.println(UUID.randomUUID());
//    }
}
