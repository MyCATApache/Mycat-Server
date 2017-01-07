package io.mycat.backend.mysql.xa.recovery;

import io.mycat.backend.mysql.xa.CoordinatorLogEntry;

import java.util.Collection;

/**
 * Created by zhangchao on 2016/10/13.
 */
public interface Repository {

    void init() ;

    void put(String id, CoordinatorLogEntry coordinatorLogEntry);

    CoordinatorLogEntry get(String coordinatorId);

    Collection<CoordinatorLogEntry> findAllCommittingCoordinatorLogEntries() ;

    Collection<CoordinatorLogEntry>  getAllCoordinatorLogEntries() ;

    void writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent) ;

    void close();

}
