package io.mycat.migrate;

import java.io.Serializable;

/**
 * Created by magicdoom on 2016/9/28.
 */
public class TaskNode implements Serializable {
    private String sql;
    private boolean end;
    private String schema;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
