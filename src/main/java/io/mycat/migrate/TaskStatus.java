package io.mycat.migrate;

import java.io.Serializable;

/**
 * Created by nange on 2016/12/7.
 */
public class TaskStatus implements Serializable {
    public int status;         //0= dump error     1=dump sucess
    public String msg;
}
