/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.config.loader;

import io.mycat.backend.PhysicalDBPool;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.ConfigUtil;
import io.mycat.server.config.ParameterMapping;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.cluster.MycatNodeConfig;
import io.mycat.server.config.node.CharsetConfig;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.HostIndexConfig;
import io.mycat.server.config.node.JdbcDriver;
import io.mycat.server.config.node.QuarantineConfig;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SequenceConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.TableConfig;
import io.mycat.server.config.node.TableConfigMap;
import io.mycat.server.config.node.UserConfig;
import io.mycat.util.SplitUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * @author mycat
 */
public class LocalLoader implements ConfigLoader {
	private static final Logger logger = LoggerFactory.getLogger("LocalLoader");
    private  Map<String, DataHostConfig> dataHosts;
    private  Map<String, DataNodeConfig> dataNodes;
    private  Map<String, SchemaConfig> schemas;
    private  Map<String, RuleConfig> tableRules;
    private  SystemConfig system;
    private  Map<String, UserConfig> users;
    private  QuarantineConfig quarantine;
    private  MycatClusterConfig cluster;
    private  CharsetConfig charsetConfig;
    private  HostIndexConfig hostIndexConfig;
    private  SequenceConfig sequenceConfig;
    
    // 为了避免原代码中频繁调用 loadRoot 去频繁读取 /mycat.dtd 和 /mycat.xml，所以将 Document 作为属性进行缓存
    private static Document document = null;
    
    public LocalLoader(){
    	this.system = new SystemConfig();
        this.users = new HashMap<String, UserConfig>();
        this.dataHosts = new HashMap<String, DataHostConfig>();
        this.dataNodes = new HashMap<String, DataNodeConfig>();
        this.schemas = new HashMap<String, SchemaConfig>();
        this.tableRules = new HashMap<String, RuleConfig>();
        this.quarantine = new QuarantineConfig();
        this.charsetConfig = new CharsetConfig();
        this.hostIndexConfig = new HostIndexConfig();
        this.sequenceConfig = new SequenceConfig();
    }
    
    private static Element loadRoot() {
        if(document == null){
        	try(InputStream dtd = ConfigFactory.class.getResourceAsStream("/mycat.dtd");
        		InputStream xml = ConfigFactory.class.getResourceAsStream("/mycat.xml")){
                document = ConfigUtil.getDocument(dtd, xml);
                return document.getDocumentElement();
            } catch (Exception e) {
            	logger.error(" loadRoot error: " + e.getMessage());
                throw new ConfigException(e);
            } 
        }
        
        return document.getDocumentElement();
    }
    
    @Override
    public UserConfig getUserConfig(String user) {
    	Element root = loadRoot();
        loadUsers(root);
        return this.users.get(user);
    }

    @Override
    public Map<String, UserConfig> getUserConfigs() {
    	Element root = loadRoot();
        loadUsers(root);
        return users;
    }

    @Override
    public SystemConfig getSystemConfig() {
    	Element root = loadRoot();
        loadSystem(root);
        return system;
    }
    @Override
    public Map<String, SchemaConfig> getSchemaConfigs() {
    	Element root = loadRoot();
		loadSchemas(root);
        return schemas;
    }
    @Override
    public SchemaConfig getSchemaConfig(String schema) {
    	Element root = loadRoot();
		loadSchemas(root);
        return schemas.get(schema);
    }
	@Override
	public Map<String, DataNodeConfig> getDataNodeConfigs() {
		Element root = loadRoot();
		loadDataNodes(root);
		return dataNodes;
	}
	@Override
	public Map<String, DataHostConfig> getDataHostConfigs() {
		Element root = loadRoot();
    	loadDataHosts(root);
		return dataHosts;
	}
	@Override
	public Map<String, RuleConfig> getTableRuleConfigs() {
		Element root = loadRoot();
		loadTableRules(root);
		return tableRules;
	}

	@Override
	public QuarantineConfig getQuarantineConfigs() {
		return quarantine;
	}
	@Override
	public MycatClusterConfig getClusterConfigs() {
		return cluster;
	}
	@Override
	public CharsetConfig getCharsetConfigs() {
		Element root = loadRoot();
		loadCharsetConfig(root);
		return this.charsetConfig;
	}
	@Override
	public HostIndexConfig getHostIndexConfig() {
		Element root = loadRoot();
		loadHostIndexConfig(root);
		return this.hostIndexConfig;
	}
	@Override
	public SequenceConfig getSequenceConfig() {
		Element root = loadRoot();
		loadSequenceConfig(root);
		return this.sequenceConfig;
	}
	
	public static Map<String, JdbcDriver> loadJdbcDriverConfig() {
		Element root = loadRoot();
		return loadJdbcDriverConfig(root);
	}

	private void loadUsers(Element root) {
        NodeList list = root.getElementsByTagName("user");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String name = e.getAttribute("name");
                UserConfig user = new UserConfig();
                user.setName(name);
                Map<String, Object> props = ConfigUtil.loadElements(e);
				user.setPassword((String) props.get("password"));
				String readOnly = (String) props.get("readOnly");
				if (null != readOnly) {
					user.setReadOnly(Boolean.valueOf(readOnly));
				}
				String schemas = (String) props.get("schemas");
                if (schemas != null) {
                    String[] strArray = SplitUtil.split(schemas, ',', true);
                    user.setSchemas(new HashSet<String>(Arrays.asList(strArray)));
                }
                if (users.containsKey(name)) {
                    throw new ConfigException("user " + name + " duplicated!");
                }
                users.put(name, user);
            }
        }
    }

    private void loadSystem(Element root) {
        NodeList list = root.getElementsByTagName("system");
        try {
        	for (int i = 0, n = list.getLength(); i < n; i++) {
                Node node = list.item(i);
                if (node instanceof Element) {
                    Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                    ParameterMapping.mapping(system, props);
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadSystem error: " + e.getMessage());
		}

    }
    private void loadSchemas(Element root) {
		NodeList list = root.getElementsByTagName("schema");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element schemaElement = (Element) list.item(i);
			String name = schemaElement.getAttribute("name");
			String dataNode = schemaElement.getAttribute("dataNode");
			String checkSQLSchemaStr = schemaElement
					.getAttribute("checkSQLschema");
			String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
			int sqlMaxLimit = -1;
			if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
				sqlMaxLimit = Integer.valueOf(sqlMaxLimitStr);

			}
			// check dataNode already exists or not
			String defaultDbType = null;
			if (dataNode != null && !dataNode.isEmpty()) {
				List<String> dataNodeLst = new ArrayList<String>(1);
				dataNodeLst.add(dataNode);
				checkDataNodeExists(dataNodeLst);
				String dataHost = dataNodes.get(dataNode).getDataHost();
				defaultDbType = dataHosts.get(dataHost).getDbType();
			} else {
				dataNode = null;
			}
			Map<String, TableConfig> tables = loadTables(schemaElement);
			if (schemas.containsKey(name)) {
				throw new ConfigException("schema " + name + " duplicated!");
			}

			// 设置了table的不需要设置dataNode属性，没有设置table的必须设置dataNode属性
			if (dataNode == null && tables.size() == 0) {
				throw new ConfigException(
						"schema "
								+ name
								+ " didn't config tables,so you must set dataNode property!");
			}

			SchemaConfig schemaConfig = new SchemaConfig(name, dataNode,
					tables, sqlMaxLimit,
					"true".equalsIgnoreCase(checkSQLSchemaStr));
			if (defaultDbType != null) {
				schemaConfig.setDefaultDataNodeDbType(defaultDbType);
				if (!"mysql".equalsIgnoreCase(defaultDbType)) {
					schemaConfig.setNeedSupportMultiDBType(true);
				}
			}

			// 判断是否有不是mysql的数据库类型，方便解析判断是否启用多数据库分页语法解析

			for (String tableName : tables.keySet()) {
				TableConfig tableConfig = tables.get(tableName);
				if (isHasMultiDbType(tableConfig)) {
					schemaConfig.setNeedSupportMultiDBType(true);
					break;
				}
			}
			Map<String, String> dataNodeDbTypeMap = new HashMap<>();
			for (String dataNodeName : dataNodes.keySet()) {
				DataNodeConfig dataNodeConfig = dataNodes.get(dataNodeName);
				String dataHost = dataNodeConfig.getDataHost();
				DataHostConfig dataHostConfig = dataHosts.get(dataHost);
				if (dataHostConfig != null) {
					String dbType = dataHostConfig.getDbType();
					dataNodeDbTypeMap.put(dataNodeName, dbType);
				}
			}
			schemaConfig.setDataNodeDbTypeMap(dataNodeDbTypeMap);
			schemas.put(name, schemaConfig);
		}
	}

	private Map<String, TableConfig> loadTables(Element node) {
		// Map<String, TableConfig> tables = new HashMap<String, TableConfig>();

		// 支持表名中包含引号[`] BEN GONG
		Map<String, TableConfig> tables = new TableConfigMap();

		NodeList nodeList = node.getElementsByTagName("table");
		for (int i = 0; i < nodeList.getLength(); i++) {
			Element tableElement = (Element) nodeList.item(i);
			String tableNameElement = tableElement.getAttribute("name").toUpperCase();
			String[] tableNames = tableNameElement.split(",");

			String primaryKey = tableElement.hasAttribute("primaryKey") ? tableElement
					.getAttribute("primaryKey").toUpperCase() : null;

			boolean autoIncrement = false;
			if (tableElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(tableElement
						.getAttribute("autoIncrement"));
			}
			boolean needAddLimit = true;
			if (tableElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(tableElement
						.getAttribute("needAddLimit"));
			}
			String tableTypeStr = tableElement.hasAttribute("type") ? tableElement
					.getAttribute("type") : null;
			int tableType = TableConfig.TYPE_GLOBAL_DEFAULT;
			if ("global".equalsIgnoreCase(tableTypeStr)) {
				tableType = TableConfig.TYPE_GLOBAL_TABLE;
			}
			String dataNode = tableElement.getAttribute("dataNode");
			RuleConfig tableRule = null;
			if (tableElement.hasAttribute("rule")) {
				String ruleName = tableElement.getAttribute("rule");
				tableRule = tableRules.get(ruleName);
				if (tableRule == null) {
					throw new ConfigException("rule " + ruleName + " is not found!");
				}
			}
			boolean ruleRequired = false;
			if (tableElement.hasAttribute("ruleRequired")) {
				ruleRequired = Boolean.parseBoolean(tableElement
						.getAttribute("ruleRequired"));
			}

			if (tableNames == null) {
				throw new ConfigException("table name is not found!");
			}
			String distPrex = "distribute(";
			boolean distTableDns = dataNode.startsWith(distPrex);
			if (distTableDns) {
				dataNode = dataNode.substring(distPrex.length(),
						dataNode.length() - 1);
			}
			for (int j = 0; j < tableNames.length; j++) {
				String tableName = tableNames[j];
				TableConfig table = new TableConfig(tableName, primaryKey,
						autoIncrement, needAddLimit, tableType, dataNode,
						getDbType(dataNode),
						(tableRule != null) ? tableRule : null,
						ruleRequired, null, false, null, null);
				checkDataNodeExists(table.getDataNodes());
				if (distTableDns) {
					distributeDataNodes(table.getDataNodes());
				}
				if (tables.containsKey(table.getName())) {
					throw new ConfigException("table " + tableName
							+ " duplicated!");
				}
				tables.put(table.getName(), table);
			}

			if (tableNames.length == 1) {
				TableConfig table = tables.get(tableNames[0]);

				// process child tables
				processChildTables(tables, table, dataNode, tableElement);
			}
		}

		return tables;
	}

	/**
	 * distribute datanodes in multi hosts,means ,dn1 (host1),dn100
	 * (host2),dn300(host3),dn2(host1),dn101(host2),dn301(host3)...etc
	 *
	 * @param dataNodes
	 */
	private void distributeDataNodes(ArrayList<String> theDataNodes) {
		Map<String, ArrayList<String>> newDataNodeMap = new HashMap<String, ArrayList<String>>(
				dataHosts.size());
		for (String dn : theDataNodes) {
			DataNodeConfig dnConf = dataNodes.get(dn);
			String host = dnConf.getDataHost();
			ArrayList<String> hostDns = newDataNodeMap.get(host);
			hostDns = (hostDns == null) ? new ArrayList<String>() : hostDns;
			hostDns.add(dn);
			newDataNodeMap.put(host, hostDns);
		}
		ArrayList<String> result = new ArrayList<String>(theDataNodes.size());
		boolean hasData = true;
		while (hasData) {
			hasData = false;
			for (ArrayList<String> dns : newDataNodeMap.values()) {
				if (!dns.isEmpty()) {
					result.add(dns.remove(0));
					hasData = true;
				}
			}
		}
		theDataNodes.clear();
		theDataNodes.addAll(result);
	}

	private Set<String> getDbType(String dataNode) {
		Set<String> dbTypes = new HashSet<>();
		String[] dataNodeArr = SplitUtil.split(dataNode, ',', '$', '-');
		for (String node : dataNodeArr) {
			DataNodeConfig datanode = dataNodes.get(node);
			DataHostConfig datahost = dataHosts.get(datanode.getDataHost());
			dbTypes.add(datahost.getDbType());
		}

		return dbTypes;
	}

	private Set<String> getDataNodeDbTypeMap(String dataNode) {
		Set<String> dbTypes = new HashSet<>();
		String[] dataNodeArr = SplitUtil.split(dataNode, ',', '$', '-');
		for (String node : dataNodeArr) {
			DataNodeConfig datanode = dataNodes.get(node);
			DataHostConfig datahost = dataHosts.get(datanode.getDataHost());
			dbTypes.add(datahost.getDbType());
		}

		return dbTypes;
	}

	private boolean isHasMultiDbType(TableConfig table) {
		Set<String> dbTypes = table.getDbTypes();
		for (String dbType : dbTypes) {
			if (!"mysql".equalsIgnoreCase(dbType)) {
				return true;
			}
		}
		return false;
	}

	private void processChildTables(Map<String, TableConfig> tables,
			TableConfig parentTable, String dataNodes, Element tableNode) {
		// parse child tables
		NodeList childNodeList = tableNode.getChildNodes();
		for (int j = 0; j < childNodeList.getLength(); j++) {
			Node theNode = childNodeList.item(j);
			if (!theNode.getNodeName().equals("childTable")) {
				continue;
			}
			Element childTbElement = (Element) theNode;

			String cdTbName = childTbElement.getAttribute("name").toUpperCase();
			String primaryKey = childTbElement.hasAttribute("primaryKey") ? childTbElement
					.getAttribute("primaryKey").toUpperCase() : null;

			boolean autoIncrement = false;
			if (childTbElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(childTbElement
						.getAttribute("autoIncrement"));
			}
			boolean needAddLimit = true;
			if (childTbElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(childTbElement
						.getAttribute("needAddLimit"));
			}
			String joinKey = childTbElement.getAttribute("joinKey")
					.toUpperCase();
			String parentKey = childTbElement.getAttribute("parentKey")
					.toUpperCase();
			TableConfig table = new TableConfig(cdTbName, primaryKey,
					autoIncrement, needAddLimit,
					TableConfig.TYPE_GLOBAL_DEFAULT, dataNodes,
					getDbType(dataNodes), null, false, parentTable, true,
					joinKey, parentKey);
			if (tables.containsKey(table.getName())) {
				throw new ConfigException("table " + table.getName()
						+ " duplicated!");
			}
			tables.put(table.getName(), table);
			processChildTables(tables, table, dataNodes, childTbElement);
		}
	}

	private void checkDataNodeExists(Collection<String> nodes) {
		if (nodes == null || nodes.size() < 1) {
			return;
		}
		for (String node : nodes) {
			if (!dataNodes.containsKey(node)) {
				throw new ConfigException("dataNode '" + node
						+ "' is not found!");
			}
		}
	}

	private void loadDataNodes(Element root) {
		NodeList list = root.getElementsByTagName("dataNode");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element element = (Element) list.item(i);
			String dnNamePre = element.getAttribute("name");

			String databaseStr = element.getAttribute("database");
			String host = element.getAttribute("dataHost");
			if (empty(dnNamePre) || empty(databaseStr) || empty(host)) {
				throw new ConfigException("dataNode " + dnNamePre
						+ " define error ,attribute can't be empty");
			}
			String[] dnNames = io.mycat.util.SplitUtil.split(dnNamePre,
					',', '$', '-');
			String[] databases = io.mycat.util.SplitUtil.split(
					databaseStr, ',', '$', '-');
			String[] hostStrings = io.mycat.util.SplitUtil.split(host,
					',', '$', '-');

			if (dnNames.length > 1
					&& dnNames.length != databases.length * hostStrings.length) {
				throw new ConfigException(
						"dataNode "
								+ dnNamePre
								+ " define error ,dnNames.length must be=databases.length*hostStrings.length");
			}
			if (dnNames.length > 1) {
				List<String[]> mhdList = mergerHostDatabase(hostStrings,
						databases);
				for (int k = 0; k < dnNames.length; k++) {
					String[] hd = mhdList.get(k);
					String dnName = dnNames[k];
					String databaseName = hd[1];
					String hostName = hd[0];
					createDataNode(dnName, databaseName, hostName);

				}

			} else {
				createDataNode(dnNamePre, databaseStr, host);
			}

		}
	}

	private List<String[]> mergerHostDatabase(String[] hostStrings,
			String[] databases) {
		List<String[]> mhdList = new ArrayList<>();
		for (int i = 0; i < hostStrings.length; i++) {
			String hostString = hostStrings[i];
			for (int i1 = 0; i1 < databases.length; i1++) {
				String database = databases[i1];
				String[] hd = new String[2];
				hd[0] = hostString;
				hd[1] = database;
				mhdList.add(hd);
			}
		}
		return mhdList;
	}

	private void createDataNode(String dnName, String database, String host) {
		DataNodeConfig conf = new DataNodeConfig(dnName, database, host);
		if (dataNodes.containsKey(conf.getName())) {
			throw new ConfigException("dataNode " + conf.getName()
					+ " duplicated!");
		}
		if (!dataHosts.containsKey(host)) {
			throw new ConfigException("dataNode " + dnName
					+ " reference dataHost:" + host + " not exists!");
		}
		dataNodes.put(conf.getName(), conf);
	}

	private boolean empty(String dnName) {
		return dnName == null || dnName.length() == 0;
	}

	private DBHostConfig createDBHostConf(String dataHost, Element node,
			String dbType, String dbDriver, int maxCon, int minCon, String filters, long logTime) {
		String nodeHost = node.getAttribute("host");
		String nodeUrl = node.getAttribute("url");
		String user = node.getAttribute("user");
		String password = node.getAttribute("password");
		String weightStr = node.getAttribute("weight");
		int weight = "".equals(weightStr) ? PhysicalDBPool.WEIGHT : Integer.valueOf(weightStr) ;
		
		String ip = null;
		int port = 0;
		if (empty(nodeHost) || empty(nodeUrl) || empty(user)) {
			throw new ConfigException(
					"dataHost "
							+ dataHost
							+ " define error,some attributes of this element is empty: "
							+ nodeHost);
		}
		if ("native".equalsIgnoreCase(dbDriver)) {
			int colonIndex = nodeUrl.indexOf(':');
			ip = nodeUrl.substring(0, colonIndex).trim();
			port = Integer.parseInt(nodeUrl.substring(colonIndex + 1).trim());
		} else {
			URI url;
			try {
				url = new URI(nodeUrl.substring(5));
			} catch (Exception e) {
				throw new ConfigException("invalid jdbc url " + nodeUrl
						+ " of " + dataHost);
			}
			ip = url.getHost();
			port = url.getPort();
		}

		DBHostConfig conf = new DBHostConfig(nodeHost, ip, port, nodeUrl, user, password);
		conf.setDbType(dbType);
		conf.setMaxCon(maxCon);
		conf.setMinCon(minCon);
		conf.setFilters(filters);
		conf.setLogTime(logTime);
		conf.setWeight(weight); 	//新增权重
		return conf;
	}

	private void loadDataHosts(Element root) {
		NodeList list = root.getElementsByTagName("dataHost");
		for (int i = 0, n = list.getLength(); i < n; ++i) {
			Element element = (Element) list.item(i);
			String name = element.getAttribute("name");
			if (dataHosts.containsKey(name)) {
				throw new ConfigException("dataHost name " + name
						+ "duplicated!");
			}
			int maxCon = Integer.valueOf(element.getAttribute("maxCon"));
			int minCon = Integer.valueOf(element.getAttribute("minCon"));
			int balance = Integer.valueOf(element.getAttribute("balance"));
			String switchTypeStr = element.getAttribute("switchType");
			int switchType = switchTypeStr.equals("") ? -1 : Integer
					.valueOf(switchTypeStr);
			String slaveThresholdStr = element.getAttribute("slaveThreshold");
			int slaveThreshold = slaveThresholdStr.equals("") ? -1 : Integer
					.valueOf(slaveThresholdStr);
			
			//如果 tempReadHostAvailable 设置大于 0 则表示写主机如果挂掉， 临时的读服务依然可用
			String tempReadHostAvailableStr = element.getAttribute("tempReadHostAvailable");
			boolean tempReadHostAvailable = tempReadHostAvailableStr.equals("") ? false : Integer.valueOf(tempReadHostAvailableStr) > 0;
			
			String writeTypStr = element.getAttribute("writeType");
			int writeType = "".equals(writeTypStr) ? PhysicalDBPool.WRITE_ONLYONE_NODE
					: Integer.valueOf(writeTypStr);

			String dbDriver = element.getAttribute("dbDriver");
			String dbType = element.getAttribute("dbType");
			String filters = element.getAttribute("filters");
			String logTimeStr = element.getAttribute("logTime");
			long logTime = "".equals(logTimeStr) ? PhysicalDBPool.LONG_TIME : Long.valueOf(logTimeStr) ;
			String heartbeatSQL = element.getElementsByTagName("heartbeat")
					.item(0).getTextContent();
			NodeList connectionInitSqlList = element
					.getElementsByTagName("connectionInitSql");
			String initConSQL = null;
			if (connectionInitSqlList.getLength() > 0) {
				initConSQL = connectionInitSqlList.item(0).getTextContent();
			}
			NodeList writeNodes = element.getElementsByTagName("writeHost");
			DBHostConfig[] writeDbConfs = new DBHostConfig[writeNodes
					.getLength()];
			Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<Integer, DBHostConfig[]>(
					2);
			for (int w = 0; w < writeDbConfs.length; w++) {
				Element writeNode = (Element) writeNodes.item(w);
				writeDbConfs[w] = createDBHostConf(name, writeNode, dbType,
						dbDriver, maxCon, minCon,filters,logTime);
				NodeList readNodes = writeNode.getElementsByTagName("readHost");
				if (readNodes.getLength() != 0) {
					DBHostConfig[] readDbConfs = new DBHostConfig[readNodes
							.getLength()];
					for (int r = 0; r < readDbConfs.length; r++) {
						Element readNode = (Element) readNodes.item(r);
						readDbConfs[r] = createDBHostConf(name, readNode,
								dbType, dbDriver, maxCon, minCon,filters,logTime);
					}
					readHostsMap.put(w, readDbConfs);
				}
			}

			DataHostConfig hostConf = new DataHostConfig(name, dbType,
					dbDriver, writeDbConfs, readHostsMap, switchType,
					slaveThreshold, tempReadHostAvailable);
			hostConf.setMaxCon(maxCon);
			hostConf.setMinCon(minCon);
			hostConf.setBalance(balance);
			hostConf.setWriteType(writeType);
			hostConf.setHeartbeatSQL(heartbeatSQL);
			hostConf.setConnectionInitSql(initConSQL);
			hostConf.setFilters(filters);
			hostConf.setLogTime(logTime);
			dataHosts.put(hostConf.getName(), hostConf);

		}
	}
	private void loadTableRules(Element root)  {
		NodeList list = root.getElementsByTagName("tableRule");
		try {
			for (int i = 0, n = list.getLength(); i < n; ++i) {
				Node node = list.item(i);
				if (node instanceof Element) {
					Element e = (Element) node;
					String name = e.getAttribute("name");
					String column = e.getAttribute("column");
					String functionName = e.getAttribute("functionName");

					if (tableRules.containsKey(name)) {
						throw new ConfigException("table rule " + name + " duplicated!");
					}
					RuleConfig ruleConfig = new RuleConfig(name, column, functionName);
					Map<String, Object> props = ConfigUtil.loadElements((Element) node);
					ruleConfig.setProps(props);

					AbstractPartitionAlgorithm function = createFunction(name, functionName);
					if (function == null) {
						throw new ConfigException("can't find function of name :" + functionName);
					}
					ParameterMapping.mapping(function, ConfigUtil.loadElements(e));
					NodeList configNodes = e.getElementsByTagName("config");
					int length = configNodes.getLength();
					if (length > 1) {
						throw new ConfigException("tableRule only one config can defined :" + name);
					}
					if(length!=0){
						Element configEle = (Element) configNodes.item(0);
						LinkedHashMap<String, Object> configs = ConfigUtil.loadLinkElements((Element) configEle);
						function.setConfig(configs);
					}

					function.init();
					ruleConfig.setRuleAlgorithm(function);
					tableRules.put(name, ruleConfig);
				}
			}
		} catch (Exception e) {
			throw new ConfigException("load tableRule error: " ,e);
		}

	}

	private AbstractPartitionAlgorithm createFunction(String name, String clazz)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException, InvocationTargetException {
		Class<?> clz = Class.forName(clazz);
		if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
			throw new IllegalArgumentException("rule function must implements "
					+ AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
		}
		return (AbstractPartitionAlgorithm) clz.newInstance();
	}

	private static Map<String, MycatNodeConfig> loadNode(Element root, int port) {
        Map<String, MycatNodeConfig> nodes = new HashMap<String, MycatNodeConfig>();
        NodeList list = root.getElementsByTagName("node");
        Set<String> hostSet = new HashSet<String>();
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element element = (Element) node;
                String name = element.getAttribute("name").trim();
                if (nodes.containsKey(name)) {
                    throw new ConfigException("node name duplicated :" + name);
                }

                Map<String, Object> props = ConfigUtil.loadElements(element);
                String host = (String) props.get("host");
                if (null == host || "".equals(host)) {
                    throw new ConfigException("host empty in node: " + name);
                }
                if (hostSet.contains(host)) {
                    throw new ConfigException("node host duplicated :" + host);
                }

                String wei = (String) props.get("weight");
                if (null == wei || "".equals(wei)) {
                    throw new ConfigException("weight should not be null in host:" + host);
                }
                int weight = Integer.valueOf(wei);
                if (weight <= 0) {
                    throw new ConfigException("weight should be > 0 in host:" + host + " weight:" + weight);
                }

                MycatNodeConfig conf = new MycatNodeConfig(name, host, port, weight);
                nodes.put(name, conf);
                hostSet.add(host);
            }
        }
        return nodes;
    }

    private static Map<String, List<String>> loadGroup(Element root, Map<String, MycatNodeConfig> nodes) {
        Map<String, List<String>> groups = new HashMap<String, List<String>>();
        NodeList list = root.getElementsByTagName("group");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Element e = (Element) node;
                String groupName = e.getAttribute("name").trim();
                if (groups.containsKey(groupName)) {
                    throw new ConfigException("group duplicated : " + groupName);
                }

                Map<String, Object> props = ConfigUtil.loadElements(e);
                String value = (String) props.get("nodeList");
                if (null == value || "".equals(value)) {
                    throw new ConfigException("group should contain 'nodeList'");
                }

                String[] sList = SplitUtil.split(value, ',', true);

                if (null == sList || sList.length == 0) {
                    throw new ConfigException("group should contain 'nodeList'");
                }

                for (String s : sList) {
                    if (!nodes.containsKey(s)) {
                        throw new ConfigException("[ node :" + s + "] in [ group:" + groupName + "] doesn't exist!");
                    }
                }
                List<String> nodeList = Arrays.asList(sList);
                groups.put(groupName, nodeList);
            }
        }
        if (!groups.containsKey("default")) {
            List<String> nodeList = new ArrayList<String>(nodes.keySet());
            groups.put("default", nodeList);
        }
        return groups;
    }

	private void loadCharsetConfig(Element root){
		NodeList list = root.getElementsByTagName("charset-config");
        try {
        	for (int i = 0, n = list.getLength(); i < n; i++) {
                Node node = list.item(i);
                if (node instanceof Element) {
                    Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                    this.charsetConfig.setProps(props);
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadCharsetConfig error: " + e.getMessage());
		}
	}
	private void loadHostIndexConfig(Element root) {
		/*NodeList list = root.getElementsByTagName("dnindex-config");
        try {
        	for (int i = 0, n = list.getLength(); i < n; i++) {
                Node node = list.item(i);
                if (node instanceof Element) {
                    Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                    this.hostIndexConfig.setProps(props);
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadHostIndexConfig error: " + e.getMessage());
		}*/
		try {
			File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
			Properties dnIndexProperties = new Properties();
			dnIndexProperties.load(new FileInputStream(file));
			this.hostIndexConfig.setProps(dnIndexProperties);
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadHostIndexConfig error: " + e.getMessage());
		}


	}
	private void loadSequenceConfig(Element root) {
		NodeList list = root.getElementsByTagName("sequence");
        try {
        	Node node = list.item(0);
    		if (node instanceof Element) {
               String type = ((Element) node).getAttribute("type");
               String vclass = ((Element) node).getAttribute("class");

               Map<String, Object> props = ConfigUtil.loadElements((Element) node);
               this.sequenceConfig.setType(type);
               this.sequenceConfig.setVclass(vclass);
               this.sequenceConfig.setProps(props);

    		}
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadSequenceConfig error: " + e.getMessage());
		}
	}
	
	private static Map<String, JdbcDriver> loadJdbcDriverConfig(Element root) {
		NodeList list = root.getElementsByTagName("driver");
        try {
        	Map<String, JdbcDriver> jdbcDriverConfig = new HashMap<>();
        	for(int i=0; i<list.getLength(); i++){
        		Node node = list.item(i);
        		if(node != null){
        			String dbType = ((Element) node).getAttribute("dbType");
                    String className = ((Element) node).getAttribute("className");
                    JdbcDriver driver = new JdbcDriver(dbType, className);
                    jdbcDriverConfig.put(dbType.toLowerCase(), driver);
        		}
        	}
        	return jdbcDriverConfig;
		} catch (Exception e) {
			e.printStackTrace();
            throw new ConfigException("loadJdbcDriverConfig error: " + e.getMessage());
		}
	}

	/**
	 * 获得 mycat.xml 解析之后的 Document 对象
	 * @return
	 */
	public static Document getDocument() {
		if(document == null)
			loadRoot();
		return document;
	}

	/**
	 * 获得  mycat.xml 的根元素 <mycat></mycat>
	 * @return
	 */
	public static Element getRoot() {
		if(document == null)
			return loadRoot();
		return document.getDocumentElement();
	}
	
	/**
	 * 重新加载 mycat.xml
	 */
	public static void reLoad(){
		document = null;
		loadRoot();
	}
}