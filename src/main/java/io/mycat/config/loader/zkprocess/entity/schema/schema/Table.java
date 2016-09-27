package io.mycat.config.loader.zkprocess.entity.schema.schema;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
 * 用于具体的表信息
* 源文件名：Table.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "table")
public class Table implements Named {

    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected String nameSuffix;
    @XmlAttribute(required = true)
    protected String dataNode;
    @XmlAttribute
    protected String rule;
    @XmlAttribute
    protected Boolean ruleRequired;
    @XmlAttribute
    protected String primaryKey;
    @XmlAttribute
    protected Boolean autoIncrement;
    @XmlAttribute
    protected Boolean needAddLimit;
    @XmlAttribute
    protected String type;

    /**
     * 子节点信息
    * @字段说明 childTable
    */
    protected List<ChildTable> childTable;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
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

    public String getNameSuffix() {
        return nameSuffix;
    }

    public void setNameSuffix(String nameSuffix) {
        this.nameSuffix = nameSuffix;
    }

    public Boolean isRuleRequired() {
        return ruleRequired;
    }

    public void setRuleRequired(Boolean ruleRequired) {
        this.ruleRequired = ruleRequired;
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

    public Boolean isNeedAddLimit() {
        return needAddLimit;
    }

    public void setNeedAddLimit(Boolean needAddLimit) {
        this.needAddLimit = needAddLimit;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Table [name=");
        builder.append(name);
        builder.append(", nameSuffix=");
        builder.append(nameSuffix);
        builder.append(", dataNode=");
        builder.append(dataNode);
        builder.append(", rule=");
        builder.append(rule);
        builder.append(", ruleRequired=");
        builder.append(ruleRequired);
        builder.append(", primaryKey=");
        builder.append(primaryKey);
        builder.append(", autoIncrement=");
        builder.append(autoIncrement);
        builder.append(", needAddLimit=");
        builder.append(needAddLimit);
        builder.append(", type=");
        builder.append(type);
        builder.append(", childTable=");
        builder.append(childTable);
        builder.append("]");
        return builder.toString();
    }

}
