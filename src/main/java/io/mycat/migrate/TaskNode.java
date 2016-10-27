package io.mycat.migrate;

import java.io.Serializable;

/**
 * Created by magicdoom on 2016/9/28.
 */
public class TaskNode implements Serializable {
    public String sql;
    public boolean end;
    public String schema;
}
