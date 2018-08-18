package io.mycat.backend.mysql.xa;

import java.io.Serializable;

/**
 * 参与者日志
 * Created by zhangchao on 2016/10/17.
 */
public class ParticipantLogEntry implements Serializable {

    private static final long serialVersionUID = 1728296701394899871L;

    /**
     * The ID of the global transaction as known by the transaction core.
     * 已知事务核心的全局事务的ID。
     */
    public String coordinatorId;

    /**
     * Identifies the participant within the global transaction.
     * 标识全局事务中的参与者。
     */
    public String uri;

    /**
     * When does this participant expire (expressed in millis since Jan 1, 1970)?
     * 参与者过期时间（自1970年1月1日以毫表示）
     */
    public long expires;

    /**
     * Best-known state of the participant.
     * 参与者最知名的状态。
     */
    public int txState;

    /**
     * For diagnostic purposes, null if not relevant.
     * 出于诊断目的，如果不相关，则为null。
     */
    public String resourceName;

    public ParticipantLogEntry(String coordinatorId, String uri,
                               long expires, String resourceName, int txState) {
        this.coordinatorId = coordinatorId;
        this.uri = uri;
        this.expires = expires;
        this.resourceName = resourceName;
        this.txState = txState;
    }

    @Override
    public boolean equals(Object other) {
        boolean ret = false;
        if (other instanceof ParticipantLogEntry) {
            ParticipantLogEntry o = (ParticipantLogEntry) other;
            if (o.coordinatorId.equals(coordinatorId) && o.uri.equals(uri)) ret = true;
        }
        return ret;
    }

    @Override
    public int hashCode() {
        return coordinatorId.hashCode();
    }

    @Override
    public String toString() {
        return "ParticipantLogEntry [id=" + coordinatorId
                + ", uri=" + uri + ", expires=" + expires
                + ", state=" + txState + ", resourceName=" + resourceName + "]";
    }


}
