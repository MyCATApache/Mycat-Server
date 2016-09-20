package io.mycat.config.loader.zkprocess.entity.schema.datanode;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * <dataNode name="dn1" dataHost="localhost1" database="db1" />
* 源文件名：DataNode.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dataNode")
public class DataNode implements Named {

    @XmlAttribute(required = true)
    private String name;

    @XmlAttribute(required = true)
    private String dataHost;

    @XmlAttribute(required = true)
    private String database;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataHost() {
        return dataHost;
    }

    public void setDataHost(String dataHost) {
        this.dataHost = dataHost;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DataNode [name=");
        builder.append(name);
        builder.append(", dataHost=");
        builder.append(dataHost);
        builder.append(", database=");
        builder.append(database);
        builder.append("]");
        return builder.toString();
    }

}
