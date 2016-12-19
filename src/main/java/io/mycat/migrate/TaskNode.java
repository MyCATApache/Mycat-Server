package io.mycat.migrate;

import java.io.Serializable;

/**
 * Created by magicdoom on 2016/9/28.
 */
public class TaskNode implements Serializable {
    private String sql;
    private int status ;    //0=init    1=start    2=sucess  end  3=error   end
    private String schema;
    private int totalTask;   //总的任务数
    private int readyTask;   //准备好可以切换的任务数量

    public int getTotalTask() {
        return totalTask;
    }

    public void setTotalTask(int totalTask) {
        this.totalTask = totalTask;
    }

    public int getReadyTask() {
        return readyTask;
    }

    public void setReadyTask(int readyTask) {
        this.readyTask = readyTask;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
