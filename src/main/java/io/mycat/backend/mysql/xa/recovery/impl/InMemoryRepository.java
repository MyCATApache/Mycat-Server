package io.mycat.backend.mysql.xa.recovery.impl;

import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.recovery.Repository;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * XA 协调者日志 内存存储
 *
 * Created by zhangchao on 2016/10/18.
 */
public class InMemoryRepository implements Repository {

    /**
     * XA 协调者日志集合
     * key ：XA 事务id
     */
    private Map<String, CoordinatorLogEntry> storage = new ConcurrentHashMap<>();
    private boolean closed = true;

    @Override
    public void init() {
        closed = false;
    }

    /**
     * 添加 XA 协调者日志
     *
     * @param id XD 事务id
     * @param coordinatorLogEntry XA 协调者日志
     */
    @Override
    public synchronized void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
        storage.put(id, coordinatorLogEntry);
    }

    /**
     * 获得 XA 协调者日志
     *
     * @param coordinatorId XD 事务id
     * @return coordinatorLogEntry XA 协调者日志
     */
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
        closed = true;
    }

    @Override
    public Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries() {
        return storage.values();
    }

    /**
     * 写入新的 XA 协调者日志集合，并清空原有数据
     *
     * @param checkpointContent XA 协调者日志集合
     */
    @Override
    public void writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) {
        storage.clear();
        for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
            storage.put(coordinatorLogEntry.id, coordinatorLogEntry);
        }
    }

    public boolean isClosed() {
        return closed;
    }

}
