package io.mycat.migrate;

import java.io.Serializable;

/**
 * Created by nange on 2016/12/7.
 */
public class TaskStatus implements Serializable {
    private int status;         //0= dump error     1=dump sucess
    private String msg;
    private String binlogFile;
    private int pos;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getBinlogFile() {
        return binlogFile;
    }

    public void setBinlogFile(String binlogFile) {
        this.binlogFile = binlogFile;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }
}
