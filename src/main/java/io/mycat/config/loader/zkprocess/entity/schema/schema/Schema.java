package io.mycat.config.loader.zkprocess.entity.schema.schema;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * <schema name="TESTDB" checkSQLschema="false" sqlMaxLimit="100">
 * * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
 * *
 * </schema>
 * 
* 源文件名：Schema.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "schema")
public class Schema implements Named {

    /**
     * schema的名称
    * @字段说明 name
    */
    @XmlAttribute(required = true)
    protected String name;

    /**
     * 当诠值讴置为 true 时，
     * 如果我们执行询句**select * from TESTDB.travelrecord;
     * **则MyCat会把询句修改为**select * from travelrecord;**
    * @字段说明 checkSQLschema
    */
    @XmlAttribute
    protected Boolean checkSQLschema;

    /**
     * 当诠值设置为某个数值时。每条执行癿SQL询句，如果没有加上limit询句，MyCat也会自劢癿加上所对应癿
    * @字段说明 sqlMaxLimit
    */
    @XmlAttribute
    protected Integer sqlMaxLimit;

    /**
     * 诠属性用二绊定逡辑库刡某个具体癿database上，
     * 1.3版本如果配置了dataNode，则不可以配置分片表，
     * 1.4可以配置默讣分片，叧雹要配置需要分片的表即可
    * @字段说明 dataNode
    */
    @XmlAttribute
    protected String dataNode;

    /**
     * 配制表信息
    * @字段说明 table
    */
    protected List<Table> table;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean isCheckSQLschema() {
        return checkSQLschema;
    }

    public void setCheckSQLschema(Boolean checkSQLschema) {
        this.checkSQLschema = checkSQLschema;
    }

    public Integer getSqlMaxLimit() {
        return sqlMaxLimit;
    }

    public void setSqlMaxLimit(Integer sqlMaxLimit) {
        this.sqlMaxLimit = sqlMaxLimit;
    }

    public String getDataNode() {
        return dataNode;
    }

    public void setDataNode(String dataNode) {
        this.dataNode = dataNode;
    }

    public List<Table> getTable() {
        if (this.table == null) {
            table = new ArrayList<>();
        }
        return table;
    }

    public void setTable(List<Table> table) {
        this.table = table;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Schema [name=");
        builder.append(name);
        builder.append(", checkSQLschema=");
        builder.append(checkSQLschema);
        builder.append(", sqlMaxLimit=");
        builder.append(sqlMaxLimit);
        builder.append(", dataNode=");
        builder.append(dataNode);
        builder.append(", table=");
        builder.append(table);
        builder.append("]");
        return builder.toString();
    }

}
