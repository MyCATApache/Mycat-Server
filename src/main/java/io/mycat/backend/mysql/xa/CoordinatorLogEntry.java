package io.mycat.backend.mysql.xa;

import io.mycat.util.TimeUtil;

import java.io.Serializable;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class CoordinatorLogEntry implements Serializable {

    private static final long serialVersionUID = -919666492191340531L;

    public final String id;

//    public final boolean wasCommitted;
    public long createTime;
    public final ParticipantLogEntry[] participants;


    public CoordinatorLogEntry(String coordinatorId,
                               ParticipantLogEntry[] participantDetails) {
        this(coordinatorId, false, participantDetails, null, TimeUtil.currentTimeMillis());
    }

    public CoordinatorLogEntry(String coordinatorId, boolean wasCommitted,
                               ParticipantLogEntry[] participants) {
        createTime = TimeUtil.currentTimeMillis();
        this.id = coordinatorId;
//        this.wasCommitted = wasCommitted;
        this.participants = participants;
    }

    public CoordinatorLogEntry(String coordinatorId, boolean wasCommitted,
                               ParticipantLogEntry[] participants, String superiorCoordinatorId, long creteTime) {
        this.createTime = creteTime;
        this.id = coordinatorId;
        //        this.wasCommitted = wasCommitted;
        this.participants = participants;
    }




}
