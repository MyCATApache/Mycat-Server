package io.mycat.backend.mysql.xa;

import java.io.Serializable;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class ParticipantLogEntry implements Serializable {

    private static final long serialVersionUID = 1728296701394899871L;

    /**
     * The ID of the global transaction as known by the transaction core.
     */

    public String coordinatorId;

    /**
     * Identifies the participant within the global transaction.
     */

    public String uri;

    /**
     * When does this participant expire (expressed in millis since Jan 1, 1970)?
     */

    public long expires;

    /**
     * Best-known state of the participant.
     */
    public int txState;

    /**
     * For diagnostic purposes, null if not relevant.
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
