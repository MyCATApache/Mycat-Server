package io.mycat.config.loader.zkprocess.entity.schema.datahost;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * <dataHost name="localhost1" maxCon="1000" minCon="10" balance="0"
     * writeType="0" dbType="mysql" dbDriver="native" switchType="1"  slaveThreshold="100">
     * </dataHost>
* 源文件名：DataHost.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dataHost")
public class DataHost implements Named {

    @XmlAttribute(required = true)
    protected Integer balance;
    @XmlAttribute(required = true)
    protected Integer maxCon;
    @XmlAttribute(required = true)
    protected Integer minCon;
    @XmlAttribute(required = true)
    protected String name;
    @XmlAttribute
    protected Integer writeType;
    @XmlAttribute
    protected Integer switchType;
    @XmlAttribute
    protected Integer slaveThreshold;
    @XmlAttribute(required = true)
    protected String dbType;
    @XmlAttribute(required = true)
    protected String dbDriver;

    @XmlAttribute()
    protected String slaveIDs;

    protected String heartbeat;
    protected String connectionInitSql;

    protected List<WriteHost> writeHost;

    public String getHeartbeat() {
        return heartbeat;
    }

    public void setHeartbeat(String heartbeat) {
        this.heartbeat = heartbeat;
    }

    public String getConnectionInitSql() {
        return connectionInitSql;
    }

    public void setConnectionInitSql(String connectionInitSql) {
        this.connectionInitSql = connectionInitSql;
    }

    public List<WriteHost> getWriteHost() {
        if (this.writeHost == null) {
            writeHost = new ArrayList<>();
        }
        return writeHost;
    }

    public String getSlaveIDs() {
        return slaveIDs;
    }

    public void setSlaveIDs(String slaveIDs) {
        this.slaveIDs = slaveIDs;
    }

    public void setWriteHost(List<WriteHost> writeHost) {
        this.writeHost = writeHost;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getMaxCon() {
        return maxCon;
    }

    public void setMaxCon(Integer maxCon) {
        this.maxCon = maxCon;
    }

    public Integer getMinCon() {
        return minCon;
    }

    public void setMinCon(Integer minCon) {
        this.minCon = minCon;
    }

    public Integer getBalance() {
        return balance;
    }

    public void setBalance(Integer balance) {
        this.balance = balance;
    }

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }

    public Integer getWriteType() {
        return writeType;
    }

    public void setWriteType(Integer writeType) {
        this.writeType = writeType;
    }

    public Integer getSwitchType() {
        return switchType;
    }

    public void setSwitchType(Integer switchType) {
        this.switchType = switchType;
    }

    public Integer getSlaveThreshold() {
        return slaveThreshold;
    }

    public void setSlaveThreshold(Integer slaveThreshold) {
        this.slaveThreshold = slaveThreshold;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DataHost [balance=");
        builder.append(balance);
        builder.append(", maxCon=");
        builder.append(maxCon);
        builder.append(", minCon=");
        builder.append(minCon);
        builder.append(", name=");
        builder.append(name);
        builder.append(", writeType=");
        builder.append(writeType);
        builder.append(", switchType=");
        builder.append(switchType);
        builder.append(", slaveThreshold=");
        builder.append(slaveThreshold);
        builder.append(", dbType=");
        builder.append(dbType);
        builder.append(", dbDriver=");
        builder.append(dbDriver);
        builder.append(", heartbeat=");
        builder.append(heartbeat);
        builder.append(", connectionInitSql=");
        builder.append(connectionInitSql);
        builder.append(", writeHost=");
        builder.append(writeHost);
        builder.append("]");
        return builder.toString();
    }

}
