package io.mycat.backend.mysql.xa;

import java.io.Serializable;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class CoordinatorLogEntry implements Serializable {

    private static final long serialVersionUID = -919666492191340531L;

    public final String id;

//    public final boolean wasCommitted;

    public final ParticipantLogEntry[] participants;


    public CoordinatorLogEntry(String coordinatorId,
                               ParticipantLogEntry[] participantDetails) {
        this(coordinatorId, false, participantDetails, null);
    }

    public CoordinatorLogEntry(String coordinatorId, boolean wasCommitted,
                               ParticipantLogEntry[] participants) {
        this.id = coordinatorId;
//        this.wasCommitted = wasCommitted;
        this.participants = participants;
    }

    public CoordinatorLogEntry(String coordinatorId, boolean wasCommitted,
                               ParticipantLogEntry[] participants, String superiorCoordinatorId) {
        this.id = coordinatorId;
//        this.wasCommitted = wasCommitted;
        this.participants = participants;
    }




}
