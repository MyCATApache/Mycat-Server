package io.mycat.backend.mysql.xa;

import io.mycat.backend.mysql.nio.handler.MultiNodeCoordinator;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangchao on 2016/10/18.
 */
public class XARollbackCallback implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(XARollbackCallback.class);

    private final  String xaId;
    private final ParticipantLogEntry participantLogEntry;

    public XARollbackCallback(String xaId, ParticipantLogEntry participantLogEntry) {
        this.xaId = xaId;
        this.participantLogEntry = participantLogEntry;
    }

    public void onResult(SQLQueryResult<Map<String, String>> result) {
        //		SQLQueryResult<Map<String, String>> queryRestl=new SQLQueryResult<Map<String, String>>(this.result,!failed, dataNode,errorMsg);
        if(result.isSuccess()) {
            LOGGER.debug("onResult success on xa rollback {},{}", xaId, participantLogEntry.resourceName);
            //recovery log
            CoordinatorLogEntry coordinatorLogEntry = MultiNodeCoordinator.inMemoryRepository.get(xaId);
            for(int i=0; i<coordinatorLogEntry.participants.length;i++){
                if(coordinatorLogEntry.participants[i].resourceName.equals(participantLogEntry.resourceName)){
                    coordinatorLogEntry.participants[i].txState = TxState.TX_ROLLBACKED_STATE;
                }
            }
            MultiNodeCoordinator.inMemoryRepository.put(xaId, coordinatorLogEntry);
            MultiNodeCoordinator.fileRepository.writeCheckpoint(xaId, MultiNodeCoordinator.inMemoryRepository.getAllCoordinatorLogEntries());

        } else {
            String errorMsg = result.getErrMsg();
            LOGGER.error( errorMsg );
            if(errorMsg.indexOf("Unknown XID") > -1) {
                //todo unknow xaId
                CoordinatorLogEntry coordinatorLogEntry = MultiNodeCoordinator.inMemoryRepository.get(xaId);
                for(int i=0; i<coordinatorLogEntry.participants.length;i++){
                    if(coordinatorLogEntry.participants[i].resourceName.equals(participantLogEntry.resourceName)){
                        coordinatorLogEntry.participants[i].txState = TxState.TX_ROLLBACKED_STATE;
                    }
                }
                MultiNodeCoordinator.inMemoryRepository.put(xaId,coordinatorLogEntry);
                MultiNodeCoordinator.fileRepository.writeCheckpoint(xaId, MultiNodeCoordinator.inMemoryRepository.getAllCoordinatorLogEntries());
            }

        }

        LOGGER.debug("[CALLBACK][XA ROLLBACK] when Mycat start");


    }
}
