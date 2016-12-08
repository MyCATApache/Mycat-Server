package io.mycat.migrate;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class BinlogStream {

    private static Logger logger = LoggerFactory.getLogger(BinlogStream.class);

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;

    private BinaryLogClient binaryLogClient;



    private volatile boolean groupEventsByTX = true;

    private Set<Long> ignoredServerIds = new HashSet<Long>();
    private Set<String> ignoredTables = new HashSet<String>();

    public BinlogStream(String username, String password) {
        this("localhost", 3306, username, password);
    }

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

        private boolean transactionInProgress;

        @Override
        public void onEvent(Event event) {
            // todo: do something about schema changes
            EventType eventType = event.getHeader().getEventType();
            switch (eventType) {
                case TABLE_MAP:
                    TableMapEventData tableMapEventData = event.getData();
                    tablesById.put(tableMapEventData.getTableId(), tableMapEventData);
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
            System.out.println(event);
        }

        private void handleUpdateRowsEvent(Event event) {
            UpdateRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            System.out.println(event);
        }

        private void handleDeleteRowsEvent(Event event) {
            DeleteRowsEventData eventData = event.getData();
            TableMapEventData tableMapEvent = tablesById.get(eventData.getTableId());
            System.out.println(event);
        }



    }
}
