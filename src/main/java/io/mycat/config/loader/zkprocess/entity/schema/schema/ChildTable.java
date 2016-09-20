package io.mycat.config.loader.zkprocess.entity.schema.schema;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * 
 * <childTable name="order_items" joinKey="order_id" parentKey="id" />
 * 配制子表信息
* 源文件名：ChildTable.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "childTable")
public class ChildTable implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute(required = true)
    protected String joinKey;
    @XmlAttribute(required = true)
    protected String parentKey;
    @XmlAttribute
    protected String primaryKey;
    @XmlAttribute
    protected Boolean autoIncrement;

    protected List<ChildTable> childTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(Boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public List<ChildTable> getChildTable() {
        if (this.childTable == null) {
            childTable = new ArrayList<>();
        }
        return childTable;
    }

    public void setChildTable(List<ChildTable> childTable) {
        this.childTable = childTable;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ChildTable [name=");
        builder.append(name);
        builder.append(", joinKey=");
        builder.append(joinKey);
        builder.append(", parentKey=");
        builder.append(parentKey);
        builder.append(", primaryKey=");
        builder.append(primaryKey);
        builder.append(", autoIncrement=");
        builder.append(autoIncrement);
        builder.append(", childTable=");
        builder.append(childTable);
        builder.append("]");
        return builder.toString();
    }

}
