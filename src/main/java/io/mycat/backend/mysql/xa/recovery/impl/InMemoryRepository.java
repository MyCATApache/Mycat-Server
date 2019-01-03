package io.mycat.backend.mysql.xa.recovery.impl;

import io.mycat.backend.mysql.nio.handler.MultiNodeCoordinator;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.ParticipantLogEntry;
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
    long count = 0 ;
    @Override
    public void init() {
        closed=false;
    }

    @Override
    public synchronized void put(String id, CoordinatorLogEntry coordinatorLogEntry) {
    	count++ ;
    	if(count > 1000){
    		count = 0;
            clear(id);
    		
    	}
        storage.put(id, coordinatorLogEntry);
    }
    private void clear(String id) {
    	Collection<CoordinatorLogEntry>  checkpointContent = storage.values();;
    	for (CoordinatorLogEntry coordinatorLogEntry : checkpointContent) {
    		ParticipantLogEntry[] participants = coordinatorLogEntry.participants;
        	boolean hasAllFinish = true;
        	for(int i = 0 ; i < participants.length; i++) {
        		if(participants[i].txState != TxState.TX_ROLLBACKED_STATE 
        				&& participants[i].txState != TxState.TX_COMMITED_STATE) {
        			hasAllFinish = false;
        			break;
        		}
        	}
            if(hasAllFinish && !id.equals(coordinatorLogEntry.id)) {
            	storage.remove(coordinatorLogEntry.id);
//                ((FileSystemRepository)MultiNodeCoordinator.fileRepository).writeStorage.remove(id);
        	}
        }
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
    public void writeCheckpoint( String id,
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
