package io.mycat.migrate;
import com.alibaba.druid.util.JdbcUtils;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static io.mycat.util.dataMigrator.DataMigratorUtil.executeQuery;

public class BinlogStream {

    private static Logger logger = LoggerFactory.getLogger(BinlogStream.class);

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;

    private BinaryLogClient binaryLogClient;

    private long slaveID;
    private String binglogFile;
    private long binlogPos;

    public long getSlaveID() {
        return slaveID;
    }

    public void setSlaveID(long slaveID) {
        this.slaveID = slaveID;
    }

    public String getBinglogFile() {
        return binglogFile;
    }

    public void setBinglogFile(String binglogFile) {
        this.binglogFile = binglogFile;
    }

    public long getBinlogPos() {
        return binlogPos;
    }

    public void setBinlogPos(long binlogPos) {
        this.binlogPos = binlogPos;
    }

    private volatile boolean groupEventsByTX = true;

    private Set<Long> ignoredServerIds = new HashSet<Long>();
    private Set<String> ignoredTables = new HashSet<String>();



    public BinlogStream(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public void setGroupEventsByTX(boolean groupEventsByTX) {
        this.groupEventsByTX = groupEventsByTX;
    }



    public void setIgnoredHostsIds(Set<Long> ignoredServerIds) {
        this.ignoredServerIds = ignoredServerIds;
    }

    public void setIgnoredTables(Set<String> ignoredTables) {
        this.ignoredTables = ignoredTables;
    }


    public void connect() throws IOException {
        allocateBinaryLogClient().connect();
    }


    public void connect(long timeoutInMilliseconds) throws IOException, TimeoutException {
        allocateBinaryLogClient().connect(timeoutInMilliseconds);
    }

    private synchronized BinaryLogClient allocateBinaryLogClient() {
        if (isConnected()) {
            throw new IllegalStateException("MySQL replication stream is already open");
        }
        binaryLogClient = new BinaryLogClient(hostname, port, username, password);
        binaryLogClient.setBinlogFilename(getBinglogFile());
        binaryLogClient.setBinlogPosition(getBinlogPos());
        binaryLogClient.setServerId(getSlaveID());
        binaryLogClient.registerEventListener(new DelegatingEventListener());
        return binaryLogClient;
    }




    public synchronized boolean isConnected() {
        return binaryLogClient != null && binaryLogClient.isConnected();
    }





    public synchronized void disconnect() throws IOException {
        if (binaryLogClient != null) {
            binaryLogClient.disconnect();
            binaryLogClient = null;
        }
    }





    private final class DelegatingEventListener implements BinaryLogClient.EventListener {

        private final Map<Long, TableMapEventData> tablesById = new HashMap<Long, TableMapEventData>();
        private final Map<Long, List<Map<String, Object>>> tablesColumnMap = new HashMap<>();

        private boolean transactionInProgress;
        private String binlogFilename;

        //当发现ddl语句时 需要更新重新取列名
        private List<Map<String, Object>> loadColumn(String database,String table)
        {
            List<Map<String, Object>> list=null;
            Connection con = null;
            try {
                con =  DriverManager.getConnection("jdbc:mysql://"+hostname,username,password);
                list = executeQuery(con, "select  COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_SET_NAME from INFORMATION_SCHEMA.COLUMNS where table_name='"+table+"' and TABLE_SCHEMA='"+database+"'");

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }finally{
                JdbcUtils.close(con);
            }
            return list;
        }

        @Override
        public void onEvent(Event event) {
            // todo: do something about schema changes
          // System.out.println(event);
            EventType eventType = event.getHeader().getEventType();
            switch (eventType) {
                case TABLE_MAP:
                    TableMapEventData tableMapEventData = event.getData();
                    tablesById.put(tableMapEventData.getTableId(), tableMapEventData);
                    if(!tablesColumnMap.containsKey(tableMapEventData.getTableId())) {
                        tablesColumnMap.put(tableMapEventData.getTableId(),loadColumn(tableMapEventData.getDatabase(),tableMapEventData.getTable())) ;
                    }
                    break;
                case ROTATE:
                    RotateEventData data=    event.getData()  ;
                    binlogFilename=data.getBinlogFilename();
                    break;
                case PRE_GA_WRITE_ROWS:
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    handleWriteRowsEvent(event);
                    break;
                case PRE_GA_UPDATE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    handleUpdateRowsEvent(event);
                    break;
                case PRE_GA_DELETE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    handleDeleteRowsEvent(event);
                    break;
                case QUERY:
                    if (groupEventsByTX) {
                        QueryEventData queryEventData = event.getData();
                        String query = queryEventData.getSql();
                        if ("BEGIN".equals(query)) {
                            transactionInProgress = true;
                        }
                    }
                    break;
                case XID:
                    if (groupEventsByTX) {
                        transactionInProgress = false;
                    }
                    break;
                default:
                    // ignore
            }
        }

        private void handleWriteRowsEvent(Event event) {
            WriteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            List<Map<String, Object>> xxx=    tablesColumnMap.get(eventData.getTableId());
            System.out.println(tableMapEvent);
            System.out.println(event);
        }

        private void handleUpdateRowsEvent(Event event) {
            UpdateRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            List<Map<String, Object>> xxx=    tablesColumnMap.get(eventData.getTableId());
            System.out.println(tableMapEvent);
            System.out.println(event);
        }

        private void handleDeleteRowsEvent(Event event) {
            DeleteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            System.out.println(tableMapEvent);
            System.out.println(event);
        }



    }

    public static void main(String[] args) {
        BinlogStream  stream=new BinlogStream("localhost",3306,"czn","MUXmux");
        try {
            stream.setSlaveID(23511);
            stream.setBinglogFile("NANGE-PC-bin.000025");
            stream.setBinlogPos(4);
            stream.connect();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
