package org.opencloudb.config.loader.zookeeper.entitiy;


import javax.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = "http://org.opencloudb/", name = "rule") public class Rules {
    protected List<TableRule> tableRule;
    protected List<Function> function;

    public List<TableRule> getTableRule() {
        if (this.tableRule == null) {
            tableRule = new ArrayList<>();
        }
        return tableRule;
    }

    public void setTableRule(List<TableRule> tableRule) {
        this.tableRule = tableRule;
    }

    public List<Function> getFunction() {
        if (this.function == null) {
            function = new ArrayList<>();
        }
        return function;
    }

    public void setFunction(List<Function> function) {
        this.function = function;
    }

    /**
     * <tableRule name="rule1">
     * * *<rule>
     * * * *<columns>id</columns>
     * * * *<algorithm>func1</algorithm>
     * * </rule>
     * </tableRule>
     */
    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "tableRule") public static class TableRule
        implements Named {
        @XmlElement(required = true, name = "rule") protected Rule rule;
        @XmlAttribute(required = true) protected String name;

        public Rule getRule() {
            return rule;
        }

        public TableRule setRule(Rule rule) {
            this.rule = rule;
            return this;
        }

        public String getName() {
            return name;
        }

        public TableRule setName(String name) {
            this.name = name;
            return this;
        }

        /**
         * *<rule>
         * * *<columns>id</columns>
         * * *<algorithm>func1</algorithm>
         * * </rule>
         */
        @XmlAccessorType(XmlAccessType.FIELD)
        @XmlType(name = "rule", propOrder = {"columns", "algorithm"}) public static class Rule {
            protected String columns;
            protected String algorithm;

            public String getColumns() {
                return columns;
            }

            public Rule setColumns(String columns) {
                this.columns = columns;
                return this;
            }

            public String getAlgorithm() {
                return algorithm;
            }

            public Rule setAlgorithm(String algorithm) {
                this.algorithm = algorithm;
                return this;
            }
        }
    }


    /**
     * <function name="mod-long" class="org.opencloudb.route.function.PartitionByMod">
     * * <property name="count">3</property>
     * </function>
     */
    @XmlAccessorType(XmlAccessType.FIELD) @XmlType(name = "function") public static class Function
        implements Propertied, Named {
        @XmlAttribute(required = true) protected String name;

        @XmlAttribute(required = true, name = "class") protected String clazz;

        protected List<Property> property;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClazz() {
            return clazz;
        }

        public void setClazz(String clazz) {
            this.clazz = clazz;
        }

        public List<Property> getProperty() {
            if (this.property == null) {
                property = new ArrayList<>();
            }
            return property;
        }

        public void setProperty(List<Property> property) {
            this.property = property;
        }

        @Override public void addProperty(Property property) {
            this.getProperty().add(property);
        }
    }
}



