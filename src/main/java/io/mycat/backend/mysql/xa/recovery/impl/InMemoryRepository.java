package io.mycat.backend.mysql.xa.recovery.impl;

import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.recovery.Repository;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhangchao on 2016/10/18.
 */
public class InMemoryRepository implements Repository {

    private Map<String, CoordinatorLogEntry> storage = new ConcurrentHashMap<String, CoordinatorLogEntry>();


    private boolean closed = true;
    @Override
    public void init() {
        closed=false;
    }

    @Override
    public synchronized void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
        storage.put(id, coordinatorLogEntry);
    }

    @Override
    public synchronized CoordinatorLogEntry get(String coordinatorId) {
        return storage.get(coordinatorId);
    }

    @Override
    public synchronized Collection<CoordinatorLogEntry> findAllCommittingCoordinatorLogEntries() {
//        Set<CoordinatorLogEntry> res = new HashSet<CoordinatorLogEntry>();
//        Collection<CoordinatorLogEntry> allCoordinatorLogEntry = storage.values();
//        for (CoordinatorLogEntry coordinatorLogEntry : allCoordinatorLogEntry) {
//            if(coordinatorLogEntry.getResultingState() == TxState.TX_PREPARED_STATE){
//                res.add(coordinatorLogEntry);
//            }
//        }
//        return res;
        return null;
    }

    @Override
    public void close() {
        storage.clear();
        closed=true;
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        return storage.values();
    }

    @Override
    public void writeCheckpoint(
            Collection<CoordinatorLogEntry> checkpointContent) {
        storage.clear();
        for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
            storage.put(coordinatorLogEntry.id, coordinatorLogEntry);
        }

    }



    public boolean isClosed() {
        return closed;
    }
}
