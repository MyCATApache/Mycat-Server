package demo.catlets;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;
import org.opencloudb.config.loader.zookeeper.entitiy.Property;
import org.opencloudb.config.loader.zookeeper.entitiy.Rules;
import org.opencloudb.config.loader.zookeeper.entitiy.Schemas;
import org.opencloudb.config.loader.zookeeper.entitiy.Server;
import org.yaml.snakeyaml.Yaml;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.util.*;


/**
 * Created by lion on 12/25/15.
 */
public class XmlToYaml {
    private static JAXBContext jaxbContext;
    private static Unmarshaller unmarshaller;

    private static String MYCLUSTER_ID = "mycat-cluster-1";

    private static Map<Object, Object> serializeMap;

    public static void main(String[] args) throws JAXBException, IOException, XMLStreamException {
        jaxbContext = JAXBContext
            .newInstance(org.opencloudb.config.loader.zookeeper.entitiy.Server.class,
                org.opencloudb.config.loader.zookeeper.entitiy.Rules.class,
                org.opencloudb.config.loader.zookeeper.entitiy.Schemas.class);
        unmarshaller = jaxbContext.createUnmarshaller();

        if (args.length > 0) {
            MYCLUSTER_ID = args[0];
        }

        try (
            InputStream schemaStream = XmlToYaml.class.getResourceAsStream("/schema.xml");
            InputStream serverStream = XmlToYaml.class.getResourceAsStream("/server.xml");
            InputStream ruleStream = XmlToYaml.class.getResourceAsStream("/rule.xml");
            InputStream myidStream = XmlToYaml.class.getResourceAsStream("/myid.properties");
            FileWriter fileWriter = new FileWriter(
                XmlToYaml.class.getResource("/zk-create.yaml").getFile())

        ) {
            Preconditions.checkNotNull(myidStream, "have not myid file");
            Properties properties = new Properties();
            properties.load(myidStream);

            serializeMap = new LinkedHashMap<>();
            serializeMap.put("zkURL", properties.getProperty("zkURL"));

            Server server = loadServer(serverStream);
            serializeMap.put("mycat-cluster",
                process(loadSchema(schemaStream), loadRule(ruleStream), server));
            serializeMap.put("mycat-nodes", processServer(server, properties.getProperty("myid")));

            fileWriter.write(new Yaml().dumpAsMap(serializeMap));
        }
    }

    private static LinkedHashMap<Object, Object> processServer(Server server, String myid) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();

        map.put("name", myid);
        map.put("cluster", MYCLUSTER_ID);

        LinkedHashMap<Object, Object> systemParams = new LinkedHashMap<>();
        propertyToMap(server.getSystem().getProperty(), new PropertyRunner(systemParams) {
            @Override public boolean match(Property property) {
                return false;
            }

            @Override public void run(Property property) {
            }
        });
        map.put("systemParams", systemParams);

        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        result.put(myid, map);
        return result;
    }

    private static LinkedHashMap<Object, Object> process(Schemas schemas, Rules rules,
        Server server) {
        LinkedHashMap<Object, Object> clusterMap = new LinkedHashMap<>();
        clusterMap.put("user", processUser(server.getUser()));
        clusterMap.put("rule", processRule(rules));
        clusterMap.put("schema", processSchema(schemas));
        clusterMap.put("datanode", processDataNode(schemas));
        clusterMap.put("datahost", processDataHost(schemas));

        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        result.put(MYCLUSTER_ID, clusterMap);
        return result;
    }

    private static Map<Object, Object> processDataNode(Schemas schemas) {
        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        for (Schemas.DataNode datanode : schemas.getDataNode()) {
            LinkedHashMap<Object, Object> dataNodeMap = new LinkedHashMap<>();
            dataNodeMap.put("name", datanode.getName());
            dataNodeMap.put("database", datanode.getDatabase());
            dataNodeMap.put("dataHost", datanode.getDataHost());

            result.put(datanode.getName(), dataNodeMap);
        }
        return result;
    }

    private static Map processDataHost(Schemas schemas) {
        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();

        for (Schemas.DataHost dataHost : schemas.getDataHost()) {
            LinkedHashMap<Object, Object> dataHostMap = new LinkedHashMap<>();
            dataHostMap.put("name", dataHost.getName());
            dataHostMap.put("balance", dataHost.getBalance());
            dataHostMap.put("maxcon", dataHost.getMaxCon());
            dataHostMap.put("mincon", dataHost.getMinCon());
            dataHostMap.put("dbtype", dataHost.getDbType());
            dataHostMap.put("dbDriver", dataHost.getDbDriver());

            if (dataHost.getWriteType() != null) {
                dataHostMap.put("writeType", dataHost.getWriteType());
            }
            if (dataHost.getSwitchType() != null) {
                dataHostMap.put("switchType", dataHost.getSwitchType());
            }
            if (dataHost.getSlaveThreshold() != null) {
                dataHostMap.put("slaveThreshold", dataHost.getSlaveThreshold());
            }
            if (dataHost.getHeartbeat() != null) {
                dataHostMap.put("heartbeatSQL", dataHost.getHeartbeat());
            }
            if (dataHost.getConnectionInitSql() != null) {
                dataHostMap.put("connectionInitSql", dataHost.getConnectionInitSql());
            }

            //writehost
            for (Schemas.DataHost.WriteHost writeHost : dataHost.getWriteHost()) {
                dataHostMap.put("mysqlGroup", writeHost.getHost());
                putMysqlgroup(dataHost, writeHost);
            }

            result.put(dataHost.getName(), dataHostMap);
        }
        return result;
    }

    private static void processMysqls(Schemas.DataHost.WriteHost dataHost) {
        Object mapObj = serializeMap.get("mycat-mysqls");
        if (mapObj == null) {
            mapObj = new LinkedHashMap<>();
            serializeMap.put("mycat-mysqls", mapObj);
        }

        Map<Object, Object> map = (Map<Object, Object>) mapObj;

        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        result.put("name", dataHost.getHost());

        result.put("ip", dataHost.getUrl().split(":")[0]);
        result.put("port", dataHost.getUrl().split(":")[1]);
        result.put("user", dataHost.getUser());
        result.put("password", dataHost.getPassword());

        result.put("hostId", "host");
        result.put("zone", "fz");
        map.put(dataHost.getHost(), result);
    }

    private static void putMysqlgroup(Schemas.DataHost dataHost,
        Schemas.DataHost.WriteHost writeHost) {
        Object mapObj = serializeMap.get("mycat-mysqlgroup");
        if (mapObj == null) {
            mapObj = new LinkedHashMap<>();
            serializeMap.put("mycat-mysqlgroup", mapObj);
        }

        Map<Object, Object> map = (Map<Object, Object>) mapObj;

        ArrayList<String> serverName = new ArrayList<>();
        if (writeHost.getReadHost() != null) {
            for (Schemas.DataHost.WriteHost.ReadHost readHost : writeHost.getReadHost()) {
                serverName.add(readHost.getHost());
                processMysqls(readHost);
            }
        }
        processMysqls(writeHost);

        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        result.put("name", writeHost.getHost());
        result.put("repType", "0");
        result.put("zone", "fz");
        result.put("servers", ObjectArrays.concat(serverName.toArray(), writeHost.getHost()));
        result.put("cur-write-server", writeHost.getHost());
        result.put("auto-write-switch", Boolean.TRUE);
        result.put("heartbeatSQL", dataHost.getHeartbeat());

        map.put(writeHost.getHost(), result);
    }

    private static Map<Object, Object> processSchema(Schemas schemas) {
        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        for (Schemas.Schema schema : schemas.getSchema()) {
            LinkedHashMap<Object, Object> schemaMap = new LinkedHashMap<>();
            schemaMap.put("name", schema.getName());

            if (schema.getDataNode() != null) {
                schemaMap.put("dataNode", schema.getDataNode());
            }
            if (schema.isCheckSQLschema() != null) {
                schemaMap.put("checkSQLSchema", schema.isCheckSQLschema());
            }
            if (schema.getSqlMaxLimit() != null) {
                schemaMap.put("defaultMaxLimit", schema.getSqlMaxLimit());
            }

            for (Schemas.Schema.Table table : schema.getTable()) {
                schemaMap.put(table.getName(), processTable(table));
            }
            result.put(schema.getName(), schemaMap);
        }

        return result;
    }

    private static Map<Object, Object> processTable(Schemas.Schema.Table table) {
        LinkedHashMap<Object, Object> tableMap = new LinkedHashMap<>();
        tableMap.put("name", table.getName());
        tableMap.put("datanode", table.getDataNode());

        if (table.getRule() != null) {
            tableMap.put("ruleName", table.getRule());
        }
        if (table.getNameSuffix() != null) {
            tableMap.put("nameSuffix", table.getNameSuffix());
        }
        if (table.isRuleRequired() != null) {
            tableMap.put("ruleRequired", table.isRuleRequired());
        }
        if (table.getPrimaryKey() != null) {
            tableMap.put("primaryKey", table.getPrimaryKey());
        }
        if (table.isAutoIncrement() != null) {
            tableMap.put("autoIncrement", table.isAutoIncrement());
        }
        if (table.isNeedAddLimit() != null) {
            tableMap.put("needAddLimit", table.isNeedAddLimit());
        }
        if (table.getType() != null) {
            tableMap.put("type", table.getType());
        }

        if (table.getChildTable() != null) {
            for (Schemas.Schema.Table.ChildTable childTable : table.getChildTable()) {
                tableMap.put(childTable.getName(), processTableChild(childTable));
            }
        }
        return tableMap;
    }

    private static Map<Object, Object> processTableChild(
        Schemas.Schema.Table.ChildTable childTable) {
        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        result.put("name", childTable.getName());
        result.put("joinKey", childTable.getJoinKey());
        result.put("parentKey", childTable.getParentKey());

        if (childTable.getPrimaryKey() != null) {
            result.put("primaryKey", childTable.getPrimaryKey());
        }
        if (childTable.isAutoIncrement() != null) {
            result.put("autoIncrement", childTable.isAutoIncrement());
        }

        if (childTable.getChildTable() != null) {
            for (Schemas.Schema.Table.ChildTable innerChildTable : childTable.getChildTable()) {
                result.put(innerChildTable.getName(), processTableChild(innerChildTable));
            }
        }
        return result;
    }

    private static Map<Object, Object> processRule(Rules rules) {
        Map<Object, Object> result = new LinkedHashMap<>();

        final LinkedHashMap<String, Rules.Function> functionMap = new LinkedHashMap<>();
        for (Rules.Function name : rules.getFunction()) {
            functionMap.put(name.getName().toUpperCase(), name);
        }


        for (Rules.TableRule tableRule : rules.getTableRule()) {
            Rules.Function fuc = functionMap.get(tableRule.getRule().getAlgorithm().toUpperCase());

            final Map<Object, Object> tableRuleMap = new LinkedHashMap<>();
            tableRuleMap.put("name", fuc.getName());
            tableRuleMap.put("functionName", fuc.getClazz());
            tableRuleMap.put("column", tableRule.getRule().getColumns());

            //load rule local configuration.
            propertyToMap(fuc.getProperty(), new PropertyRunner(tableRuleMap) {
                @Override public boolean match(Property property) {
                    return property.getName().toUpperCase().equals("MAPFILE");
                }

                @Override public void run(Property property) {
                    try (InputStream inputStream = getClass()
                        .getResourceAsStream("/" + property.getValue());
                        BufferedReader reader = new BufferedReader(
                            new InputStreamReader(inputStream))) {

                        String line;
                        Map<Object, Object> configMap = new LinkedHashMap<>();
                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith("#") || line.trim().equals("")) {
                                continue;
                            }

                            String[] equals = line.split("=");
                            configMap.put(equals[0], equals[1]);
                        }
                        tableRuleMap.put("config", configMap);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            result.put(tableRule.getName(), tableRuleMap);
        }
        return result;
    }

    private static Map<Object, Object> processUser(List<Server.User> userList) {
        Map<Object, Object> result = new LinkedHashMap<>();

        for (Server.User user : userList) {
            Map<Object, Object> userMap = new LinkedHashMap<>();
            userMap.put("name", user.getName());
            propertyToMap(user.getProperty(), new PropertyRunner(userMap) {
                @Override public boolean match(Property property) {
                    return property.getName().equals("schemas");
                }

                @Override public void run(Property property) {
                    result.put(property.getName(), property.getValue().split(","));
                }
            });
            result.put(user.getName(), userMap);
        }
        return result;
    }

    private static void propertyToMap(List<Property> properties, PropertyRunner runner) {
        for (Property property : properties) {
            if (runner.match(property)) {
                runner.run(property);
            } else {
                runner.getResult().put(property.getName(), property.getValue());
            }
        }
    }

    private static Schemas loadSchema(InputStream inputStream)
        throws JAXBException, XMLStreamException {
        XMLStreamReader xsr = getXmlStreamReader(inputStream, "schema.xml");
        return (Schemas) unmarshaller.unmarshal(xsr);
    }

    private static Rules loadRule(InputStream inputStream)
        throws JAXBException, XMLStreamException {
        XMLStreamReader xsr = getXmlStreamReader(inputStream, "rule.xml");
        return (Rules) unmarshaller.unmarshal(xsr);
    }

    private static Server loadServer(InputStream inputStream)
        throws JAXBException, XMLStreamException {
        XMLStreamReader xsr = getXmlStreamReader(inputStream, "server.xml");
        return (Server) unmarshaller.unmarshal(xsr);
    }

    private static XMLStreamReader getXmlStreamReader(InputStream inputStream, String fileName)
        throws XMLStreamException {
        Preconditions.checkNotNull(inputStream, fileName + " is not exist.");

        XMLInputFactory xif = XMLInputFactory.newFactory();
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        return xif.createXMLStreamReader(new StreamSource(inputStream));
    }

    public static abstract class PropertyRunner {

        Map<Object, Object> result;

        public PropertyRunner(Map<Object, Object> result) {
            this.result = result;
        }

        abstract public boolean match(Property property);

        abstract public void run(Property property);

        public Map<Object, Object> getResult() {
            return result;
        }

        public void setResult(Map<Object, Object> result) {
            this.result = result;
        }
    }


    public static abstract class Process<I> {
        public abstract void processort(I i);
    }
}
