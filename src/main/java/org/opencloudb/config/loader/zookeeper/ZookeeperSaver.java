package org.opencloudb.config.loader.zookeeper;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opencloudb.config.loader.zookeeper.entitiy.*;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.util.ConfigException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * save data loaded from  zookeeper to xml file.
 */
public class ZookeeperSaver {
    private final static Joiner commaJonier = Joiner.on(",");
    private JAXBContext jaxbContext;

    public ZookeeperSaver() throws Exception {
        super();
        this.jaxbContext = JAXBContext
            .newInstance(org.opencloudb.config.loader.zookeeper.entitiy.Server.class,
                org.opencloudb.config.loader.zookeeper.entitiy.Rules.class,
                org.opencloudb.config.loader.zookeeper.entitiy.Schemas.class);
    }

    public void saveConfig(JSONObject jsonObject) throws Exception {
        saveServer(jsonObject, "server");
        saveRule(jsonObject, "rule");
        saveSchema(jsonObject, "schema");
    }


    Schemas saveSchema(JSONObject jsonObject, String fileName) throws Exception {
        JSONObject cluster = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);
        Preconditions.checkNotNull(cluster);

        Schemas schemas = new Schemas();

        schemas.setSchema(createSchema(cluster));
        schemas.setDataNode(createDataNode(cluster));
        schemas.setDataHost(createDataHost(jsonObject));

        //save to file, /MYCAT_HOME/conf/${fileName}.xml
        marshaller(schemas, getConfigPath() + fileName + ".xml", "schema");
        return schemas;
    }

    private List<Schemas.Schema> createSchema(JSONObject cluster) throws IOException {
        JSONObject schemasJson = cluster.getJSONObject("schema");
        Preconditions.checkNotNull(schemasJson);

        List<Schemas.Schema> schemas = new ArrayList<>();
        for (String key : schemasJson.keySet()) {
            JSONObject schemaJson = schemasJson.getJSONObject(key);

            Schemas.Schema schema = new Schemas.Schema();
            schema.setName(schemaJson.getString("name"));
            schema.setDataNode(schemaJson.optString("dataNode", null));
            schema.setSqlMaxLimit(schemaJson.optInt("defaultMaxLimit", 100));

            if (schemaJson.has("checkSQLSchema")) {
                schema.setCheckSQLschema(schemaJson.optBoolean("checkSQLSchema"));
            }

            schema.setTable(createSchemaTables(schemaJson));

            schemas.add(schema);
        }
        return schemas;
    }

    private List<Schemas.Schema.Table> createSchemaTables(JSONObject tablesJson)
        throws IOException {
        List<Schemas.Schema.Table> tables = new ArrayList<>();

        for (String tableKey : tablesJson.keySet()) {
            //skip all none table configuration.
            if (!(tablesJson.get(tableKey) instanceof JSONObject)) {
                continue;
            }
            tables.add(createSchemaTable(tablesJson.getJSONObject(tableKey)));
        }

        return tables;
    }

    private Schemas.Schema.Table createSchemaTable(JSONObject tablesJson) {
        Schemas.Schema.Table table = new Schemas.Schema.Table();

        table.setName(tablesJson.getString("name"));
        table.setDataNode(tablesJson.optString("datanode", null));

        if (tablesJson.has("autoIncrement")) {
            table.setAutoIncrement(tablesJson.optBoolean("autoIncrement"));
        }
        if (tablesJson.has("needAddLimit")) {
            table.setNeedAddLimit(tablesJson.optBoolean("needAddLimit"));
        }
        if (tablesJson.has("ruleRequired")) {
            table.setRuleRequired(tablesJson.optBoolean("ruleRequired"));
        }

        table.setNameSuffix(tablesJson.optString("nameSuffix", null));
        table.setPrimaryKey(tablesJson.optString("primaryKey", null));
        table.setRule(tablesJson.optString("ruleName", null));

        //1 is global table
        if (tablesJson.has("type")) {
            String talbeType = tablesJson.get("type").toString().equals("1") ? "global" : null;
            table.setType(talbeType);
        }

        table.setChildTable(createChildTable(tablesJson));

        return table;
    }

    private List<Schemas.Schema.Table.ChildTable> createChildTable(JSONObject tablesJson) {
        List<Schemas.Schema.Table.ChildTable> childTables = new ArrayList<>();

        for (String childTableKey : tablesJson.keySet()) {
            //have child tables
            if (tablesJson.get(childTableKey) instanceof JSONObject) {
                Schemas.Schema.Table.ChildTable childTable = new Schemas.Schema.Table.ChildTable();
                JSONObject childTableJson = tablesJson.getJSONObject(childTableKey);

                childTable.setName(childTableJson.getString("name"));

                if (tablesJson.has("autoIncrement")) {
                    childTable.setAutoIncrement(childTableJson.optBoolean("autoIncrement"));
                }
                childTable.setPrimaryKey(childTableJson.optString("primaryKey", null));
                childTable.setJoinKey(childTableJson.optString("joinKey", null));
                childTable.setParentKey(childTableJson.optString("parentKey", null));

                childTable.setChildTable(createChildTable(childTableJson));

                childTables.add(childTable);
            }
        }
        return childTables;
    }

    private List<Schemas.DataNode> createDataNode(JSONObject cluster) throws IOException {
        JSONObject dataNodesJson = cluster.getJSONObject("datanode");
        Preconditions.checkNotNull(dataNodesJson);


        List<Schemas.DataNode> dataNodes = new ArrayList<>();
        for (String key : dataNodesJson.keySet()) {
            JSONObject dataNodeJson = dataNodesJson.getJSONObject(key);
            Schemas.DataNode dataNode = new Schemas.DataNode();
            dataNode.setName(dataNodeJson.getString("name"));
            dataNode.setDatabase(dataNodeJson.getString("database"));
            dataNode.setDataHost(dataNodeJson.getString("dataHost"));
            dataNodes.add(dataNode);
        }
        return dataNodes;
    }

    private List<Schemas.DataHost> createDataHost(JSONObject jsonObject) throws IOException {
        JSONObject cluster = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);
        JSONObject dataHostsJson = cluster.getJSONObject("datahost");
        Preconditions.checkNotNull(dataHostsJson);

        List<Schemas.DataHost> dataHosts = new ArrayList<>();
        for (String key : dataHostsJson.keySet()) {
            JSONObject dataHostJson = dataHostsJson.getJSONObject(key);

            Schemas.DataHost dataHost = new Schemas.DataHost();
            dataHost.setName(dataHostJson.optString("name", null));
            dataHost.setConnectionInitSql(dataHostJson.optString("connectionInitSql", null));
            dataHost.setDbDriver(dataHostJson.optString("dbDriver", null));
            dataHost.setDbType((dataHostJson.optString("dbtype", null)));
            dataHost.setHeartbeat((dataHostJson.optString("heartbeatSQL", null)));

            if (dataHostJson.has("maxcon")) {
                dataHost.setMaxCon(dataHostJson.getInt("maxcon"));
            }
            if (dataHostJson.has("mincon")) {
                dataHost.setMinCon(dataHostJson.getInt("mincon"));
            }
            if (dataHostJson.has("balance")) {
                dataHost.setBalance(dataHostJson.optInt("balance"));
            }
            if (dataHostJson.has("writetype")) {
                dataHost.setWriteType(dataHostJson.optInt("writetype"));
            }
            if (dataHostJson.has("slaveThreshold")) {
                dataHost.setSlaveThreshold(dataHostJson.optInt("slaveThreshold"));
            }

            dataHost.setWriteHost(createWriteHost(jsonObject, dataHostJson));
            dataHosts.add(dataHost);
        }
        return dataHosts;
    }

    private List<Schemas.DataHost.WriteHost> createWriteHost(JSONObject cluster,
        JSONObject dataHostJson) {
        String mysqlGroup = dataHostJson.getString("mysqlGroup");
        String dataHostName = dataHostJson.getString("name");

        JSONObject myGroupJson = cluster.getJSONObject("mysqlGroup").getJSONObject(mysqlGroup);
        JSONObject mysqls = cluster.getJSONObject("mysqls");

        String currentWriteName = myGroupJson.getString("cur-write-server");
        JSONObject currentWriteJson = mysqls.getJSONObject(currentWriteName);

        //write host
        Schemas.DataHost.WriteHost currentWrite = new Schemas.DataHost.WriteHost();
        currentWrite.setHost(dataHostName);
        currentWrite.setPassword(currentWriteJson.getString("password"));
        currentWrite
            .setUrl(currentWriteJson.getString("ip") + ":" + currentWriteJson.getInt("port"));
        currentWrite.setUser(currentWriteJson.getString("user"));

        if (currentWriteJson.has("usingDecrypt")) {
            currentWrite.setUsingDecrypt(currentWriteJson.optBoolean("usingDecrypt"));
        }

        //read host
        JSONArray allHosts = myGroupJson.getJSONArray("servers");
        List<Schemas.DataHost.WriteHost.ReadHost> readHosts = new ArrayList<>();

        for (int i = 0; i < allHosts.length(); i++) {
            String readHostName = allHosts.getString(i);
            //skip current write host.
            if (!readHostName.equals(currentWriteName)) {
                JSONObject readHostJson = mysqls.getJSONObject(readHostName);
                Schemas.DataHost.WriteHost.ReadHost readHost =
                    new Schemas.DataHost.WriteHost.ReadHost();

                readHost.setHost(readHostName);
                readHost.setPassword(readHostJson.getString("password"));
                readHost.setUrl(readHostJson.getString("ip") + ":" + readHostJson.getInt("port"));
                readHost.setUser(currentWriteJson.getString("user"));
                readHost.setWeight(readHostJson.optString("weight", null));

                if (readHostJson.has("usingDecrypt")) {
                    readHost.setUsingDecrypt(readHostJson.optBoolean("usingDecrypt"));
                }
                readHosts.add(readHost);
            }
        }

        currentWrite.setReadHost(readHosts);
        List<Schemas.DataHost.WriteHost> writeHosts = new ArrayList<>();
        writeHosts.add(currentWrite);
        return writeHosts;
    }

    Rules saveRule(JSONObject jsonObject, String fileName) throws Exception {
        JSONObject cluster = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);
        JSONObject rulesJson = cluster.getJSONObject("rule");
        Preconditions.checkNotNull(cluster);
        Preconditions.checkNotNull(rulesJson);

        Rules rules = new Rules();

        for (String key : rulesJson.keySet()) {
            JSONObject tableRuleJson = rulesJson.getJSONObject(key);

            rules.getTableRule().add(createTableRule(tableRuleJson));
            rules.getFunction().add(createFunction(tableRuleJson));
        }

        //save to file, /MYCAT_HOME/conf/${fileName}.xml
        marshaller(rules, getConfigPath() + fileName + ".xml", "rule");
        return rules;
    }

    private Rules.Function createFunction(JSONObject tableRuleJson) throws IOException {
        Rules.Function tableFunction = new Rules.Function();

        //get data and remove non-property in json.
        String name = tableRuleJson.getString("name");
        tableFunction.setName(name);
        tableFunction.setClazz(tableRuleJson.getString("functionName"));

        tableRuleJson.remove("name");
        tableRuleJson.remove("functionName");

        //json have config key,so to save it to file and set property mapFile
        if (tableRuleJson.has("config")) {
            JSONObject config = tableRuleJson.getJSONObject("config");

            //save config to file. /conf/${name}.txt
            String fileName = name + ".txt";
            Path path = Paths.get(getConfigPath() + fileName);

            try (BufferedWriter writer = Files
                .newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
            ) {
                for (String key : config.keySet()) {
                    String value = config.get(key).toString();
                    writer.write(key + "=" + value);
                    writer.newLine();
                }
            }
            //remove config and put mapFile
            tableRuleJson.remove("config");
            tableRuleJson.put("mapFile", fileName);
        }

        putProperty(tableRuleJson, tableFunction);

        return tableFunction;
    }

    /**
     * create a {@link org.opencloudb.config.loader.zookeeper.entitiy.Rules.TableRule}
     */
    private Rules.TableRule createTableRule(JSONObject tableRuleJson) {
        Rules.TableRule tableRule = new Rules.TableRule();

        //get data and remove non-property in json.
        tableRule.setName(tableRuleJson.getString("name"));

        Rules.TableRule.Rule rule = new Rules.TableRule.Rule();
        rule.setColumns(tableRuleJson.getString("column"));
        tableRuleJson.remove("column");
        rule.setAlgorithm(tableRuleJson.getString("name"));

        tableRule.setRule(rule);
        return tableRule;
    }

    Server saveServer(JSONObject jsonObject, String fileName) throws Exception {
        JSONObject myNode = jsonObject.getJSONObject(ZookeeperLoader.NODE_KEY);
        JSONObject cluster = jsonObject.getJSONObject(ZookeeperLoader.CLUSTER_KEY);
        Preconditions.checkNotNull(myNode);
        Preconditions.checkNotNull(cluster);

        JSONObject systemParams = myNode.getJSONObject("systemParams");
        JSONObject user = cluster.getJSONObject("user");

        Server server = new Server();

        //system
        Server.System serverSystem = new Server.System();
        putProperty(systemParams, serverSystem);
        server.setSystem(serverSystem);

        //user
        ArrayList<Server.User> userList = new ArrayList<>();
        if (user != null && user.length() > 0) {
            for (String key : user.keySet()) {
                Server.User serverUser = new Server.User();
                JSONObject userObject = user.getJSONObject(key);

                serverUser.setName(userObject.getString("name"));

                //ignore name and set other to properties;
                userObject.remove("name");

                putProperty(userObject, serverUser);
                userList.add(serverUser);
            }
        }
        server.setUser(userList);

        //save to file, /MYCAT_HOME/conf/${fileName}.xml
        marshaller(server, getConfigPath() + fileName + ".xml", "server");
        return server;
    }

    private String getConfigPath() {
        String homePath = SystemConfig.getHomePath();
        String confPath = File.separator + "conf" + File.separator;

        try {
            return homePath != null ? homePath + confPath : Paths.get(".").toRealPath() + confPath;
        } catch (IOException e) {
            throw new ConfigException("set home path error");
        }
    }

    private void marshaller(Object object, String filePathAndName, String dtdName)
        throws Exception {
        Marshaller marshaller = jaxbContext.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);

        marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders",
            String.format("<!DOCTYPE mycat:%1$s SYSTEM \"%1$s.dtd\">", dtdName));

        Path path = Paths.get(filePathAndName);

        try (OutputStream out = Files
            .newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            marshaller.marshal(object, out);
        }
    }

    private void putProperty(JSONObject json, Propertied propertied) {
        if (json != null && json.length() > 0) {
            for (String key : json.keySet()) {
                Object obj = json.get(key);
                if (obj instanceof JSONArray) {
                    //join value using ',' .
                    String value = commaJonier.join(json.getJSONArray(key).iterator()).trim();
                    propertied.addProperty(createProperty(key, value));
                    continue;
                }

                propertied.addProperty(createProperty(key, obj.toString()));
            }
        }
    }

    private Property createProperty(String key, String value) {
        return new Property().setName(key).setValue(value);
    }
}
