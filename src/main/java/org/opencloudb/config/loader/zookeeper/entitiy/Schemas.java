package org.opencloudb.config.loader.zookeeper.entitiy;

import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "schema") public class Schemas {
    protected List<Schema> schema;
    protected List<DataNode> dataNode;
    protected List<DataHost> dataHost;

    public List<Schema> getSchema() {
        if (this.schema == null) {
            schema = new ArrayList<>();
        }
        return schema;
    }

    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }

    public List<DataNode> getDataNode() {
        if (this.dataNode == null) {
            dataNode = new ArrayList<>();
        }
        return dataNode;
    }

    public void setDataNode(List<DataNode> dataNode) {
        this.dataNode = dataNode;
    }

    public List<DataHost> getDataHost() {
        if (this.dataHost == null) {
            dataHost = new ArrayList<>();
        }
        return dataHost;
    }

    public void setDataHost(List<DataHost> dataHost) {
        this.dataHost = dataHost;
    }

    /**
     * <schema name="TESTDB" checkSQLschema="false" sqlMaxLimit="100">
     * * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
     * *
     * </schema>
     */
    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "schema") public static class Schema
        implements Named {
        @XmlAttribute(required = true) protected String name;
        @XmlAttribute protected Boolean checkSQLschema;
        @XmlAttribute protected Integer sqlMaxLimit;
        @XmlAttribute protected String dataNode;

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

        /**
         * <table name="travelrecord" dataNode="dn1,dn2,dn3" rule="auto-sharding-long" />
         */
        @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "table") public static class Table
            implements Named {
            @XmlAttribute(required = true) protected String name;
            @XmlAttribute protected String nameSuffix;
            @XmlAttribute(required = true) protected String dataNode;
            @XmlAttribute protected String rule;
            @XmlAttribute protected Boolean ruleRequired;
            @XmlAttribute protected String primaryKey;
            @XmlAttribute protected Boolean autoIncrement;
            @XmlAttribute protected Boolean needAddLimit;
            @XmlAttribute protected String type;

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

            /**
             * <childTable name="order_items" joinKey="order_id" parentKey="id" />
             */
            @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "childTable")
            public static class ChildTable implements Named {
                @XmlAttribute(required = true) protected String name;
                @XmlAttribute(required = true) protected String joinKey;
                @XmlAttribute(required = true) protected String parentKey;
                @XmlAttribute protected String primaryKey;
                @XmlAttribute protected Boolean autoIncrement;

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
            }
        }
    }


    /**
     * <dataNode name="dn1" dataHost="localhost1" database="db1" />
     */
    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "dataNode") public static class DataNode
        implements Named {
        @XmlAttribute(required = true) protected String name;
        @XmlAttribute(required = true) protected String dataHost;
        @XmlAttribute(required = true) protected String database;

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
    }


    /**
     * <dataHost name="localhost1" maxCon="1000" minCon="10" balance="0"
     * writeType="0" dbType="mysql" dbDriver="native" switchType="1"  slaveThreshold="100">
     * </dataHost>
     */
    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "dataHost") public static class DataHost
        implements Named {
        @XmlAttribute(required = true) protected Integer balance;
        @XmlAttribute(required = true) protected Integer maxCon;
        @XmlAttribute(required = true) protected Integer minCon;
        @XmlAttribute(required = true) protected String name;
        @XmlAttribute protected Integer writeType;
        @XmlAttribute protected Integer switchType;
        @XmlAttribute protected Integer slaveThreshold;
        @XmlAttribute(required = true) protected String dbType;
        @XmlAttribute(required = true) protected String dbDriver;

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

        /**
         * <readHost host="" url="" password="" user=""></readHost>
         */
        @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "writeHost")
        public static class WriteHost {
            @XmlAttribute(required = true) protected String host;
            @XmlAttribute(required = true) protected String url;
            @XmlAttribute(required = true) protected String password;
            @XmlAttribute(required = true) protected String user;
            @XmlAttribute protected Boolean usingDecrypt;

            private List<ReadHost> readHost;

            public String getHost() {
                return host;
            }

            public void setHost(String host) {
                this.host = host;
            }

            public String getUrl() {
                return url;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            public String getPassword() {
                return password;
            }

            public void setPassword(String password) {
                this.password = password;
            }

            public String getUser() {
                return user;
            }

            public void setUser(String user) {
                this.user = user;
            }

            public Boolean isUsingDecrypt() {
                return usingDecrypt;
            }

            public void setUsingDecrypt(Boolean usingDecrypt) {
                this.usingDecrypt = usingDecrypt;
            }

            public List<ReadHost> getReadHost() {
                if (this.readHost == null) {
                    readHost = new ArrayList<>();
                }
                return readHost;
            }

            public void setReadHost(List<ReadHost> readHost) {
                this.readHost = readHost;
            }


            /**
             * <readHost host="" url="" password="" user=""></readHost>
             */
            @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "readHost")
            public static class ReadHost extends WriteHost {
                @XmlAttribute protected String weight;

                public String getWeight() {
                    return weight;
                }

                public void setWeight(String weight) {
                    this.weight = weight;
                }

                @XmlTransient @Override public List<ReadHost> getReadHost() {
                    return super.getReadHost();
                }
            }
        }
    }
}
