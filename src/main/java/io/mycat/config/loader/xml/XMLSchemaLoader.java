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
package io.mycat.config.loader.xml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.function.TableRuleAware;
import io.mycat.util.ObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.DataHostConfig;
import io.mycat.config.model.DataNodeConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.TableConfigMap;
import io.mycat.config.model.rule.TableRuleConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.config.util.ConfigUtil;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.util.DecryptUtil;
import io.mycat.util.SplitUtil;

/**
 * @author mycat
 */
@SuppressWarnings("unchecked")
public class XMLSchemaLoader implements SchemaLoader {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(XMLSchemaLoader.class);
	
	private final static String DEFAULT_DTD = "/schema.dtd";
	private final static String DEFAULT_XML = "/schema.xml";

	private final Map<String, TableRuleConfig> tableRules;
	private final Map<String, DataHostConfig> dataHosts;
	private final Map<String, DataNodeConfig> dataNodes;
	private final Map<String, SchemaConfig> schemas;

	public XMLSchemaLoader(String schemaFile, String ruleFile) {
		//先读取rule.xml
		XMLRuleLoader ruleLoader = new XMLRuleLoader(ruleFile);
		//将tableRules拿出，用于这里加载Schema做rule有效判断，以及之后的分片路由计算
		this.tableRules = ruleLoader.getTableRules();
		//释放ruleLoader
		ruleLoader = null;
		this.dataHosts = new HashMap<String, DataHostConfig>();
		this.dataNodes = new HashMap<String, DataNodeConfig>();
		this.schemas = new HashMap<String, SchemaConfig>();
		//读取加载schema配置
		this.load(DEFAULT_DTD, schemaFile == null ? DEFAULT_XML : schemaFile);
	}

	public XMLSchemaLoader() {
		this(null, null);
	}

	@Override
	public Map<String, TableRuleConfig> getTableRules() {
		return tableRules;
	}

	@Override
	public Map<String, DataHostConfig> getDataHosts() {
		return (Map<String, DataHostConfig>) (dataHosts.isEmpty() ? Collections.emptyMap() : dataHosts);
	}

	@Override
	public Map<String, DataNodeConfig> getDataNodes() {
		return (Map<String, DataNodeConfig>) (dataNodes.isEmpty() ? Collections.emptyMap() : dataNodes);
	}

	@Override
	public Map<String, SchemaConfig> getSchemas() {
		return (Map<String, SchemaConfig>) (schemas.isEmpty() ? Collections.emptyMap() : schemas);
	}

	private void load(String dtdFile, String xmlFile) {
		InputStream dtd = null;
		InputStream xml = null;
		try {
			dtd = XMLSchemaLoader.class.getResourceAsStream(dtdFile);
			xml = XMLSchemaLoader.class.getResourceAsStream(xmlFile);
			Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
			//先加载所有的DataHost
			loadDataHosts(root);
			//再加载所有的DataNode
			loadDataNodes(root);
			//最后加载所有的Schema
			loadSchemas(root);
		} catch (ConfigException e) {
			throw e;
		} catch (Exception e) {
			throw new ConfigException(e);
		} finally {
			
			if (dtd != null) {
				try {
					dtd.close();
				} catch (IOException e) {
				}
			}
			
			if (xml != null) {
				try {
					xml.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private void loadSchemas(Element root) {
		NodeList list = root.getElementsByTagName("schema");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element schemaElement = (Element) list.item(i);
			//读取各个属性
			String name = schemaElement.getAttribute("name");
			String dataNode = schemaElement.getAttribute("dataNode");
			String checkSQLSchemaStr = schemaElement.getAttribute("checkSQLschema");
			String sqlMaxLimitStr = schemaElement.getAttribute("sqlMaxLimit");
			int sqlMaxLimit = -1;
			//读取sql返回结果集限制
			if (sqlMaxLimitStr != null && !sqlMaxLimitStr.isEmpty()) {
				sqlMaxLimit = Integer.parseInt(sqlMaxLimitStr);
			}
			
			// check dataNode already exists or not,看schema标签中是否有datanode
			String defaultDbType = null;
			//校验检查并添加dataNode
			if (dataNode != null && !dataNode.isEmpty()) {
				List<String> dataNodeLst = new ArrayList<String>(1);
				dataNodeLst.add(dataNode);
				checkDataNodeExists(dataNodeLst);
				String dataHost = dataNodes.get(dataNode).getDataHost();
				defaultDbType = dataHosts.get(dataHost).getDbType();
			} else {
				dataNode = null;
			}
			//加载schema下所有tables
			Map<String, TableConfig> tables = loadTables(schemaElement);
			//判断schema是否重复
			if (schemas.containsKey(name)) {
				throw new ConfigException("schema " + name + " duplicated!");
			}

			// 设置了table的不需要设置dataNode属性，没有设置table的必须设置dataNode属性
			if (dataNode == null && tables.size() == 0) {
				throw new ConfigException(
						"schema " + name + " didn't config tables,so you must set dataNode property!");
			}

			SchemaConfig schemaConfig = new SchemaConfig(name, dataNode,
					tables, sqlMaxLimit, "true".equalsIgnoreCase(checkSQLSchemaStr));

			//设定DB类型，这对之后的sql语句路由解析有帮助
			if (defaultDbType != null) {
				schemaConfig.setDefaultDataNodeDbType(defaultDbType);
				if (!"mysql".equalsIgnoreCase(defaultDbType)) {
					schemaConfig.setNeedSupportMultiDBType(true);
				}
			}

			// 判断是否有不是mysql的数据库类型，方便解析判断是否启用多数据库分页语法解析
			for (TableConfig tableConfig : tables.values()) {
				if (isHasMultiDbType(tableConfig)) {
					schemaConfig.setNeedSupportMultiDBType(true);
					break;
				}
			}
			//记录每种dataNode的DB类型
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

	
	/**
	 * 处理动态日期表, 支持 YYYYMM、YYYYMMDD 两种格式
	 * 
	 * YYYYMM格式： 	  yyyymm,2015,01,60   
	 * YYYYMMDD格式:  yyyymmdd,2015,01,10,50
	 * 
	 * @param tableNameElement
	 * @param tableNameSuffixElement
	 * @return
	 */
	private String doTableNameSuffix(String tableNameElement, String tableNameSuffixElement) {
		
		String newTableName = tableNameElement;
		
		String[] params = tableNameSuffixElement.split(",");			
		String suffixFormat = params[0].toUpperCase();		
		if ( suffixFormat.equals("YYYYMM") ) {
			
			//读取参数
			int yyyy = Integer.parseInt( params[1] );
			int mm = Integer.parseInt( params[2] );					
			int mmEndIdx =  Integer.parseInt( params[3] );
			
			//日期处理
			SimpleDateFormat yyyyMMSDF = new SimpleDateFormat("yyyyMM"); 
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, yyyy );
			cal.set(Calendar.MONTH, mm - 1 );
			cal.set(Calendar.DATE, 0 );
			
			//表名改写
			StringBuffer tableNameBuffer = new StringBuffer();
			for(int mmIdx = 0; mmIdx <= mmEndIdx; mmIdx++) {						
				tableNameBuffer.append( tableNameElement );
				tableNameBuffer.append( yyyyMMSDF.format(cal.getTime()) );							
				cal.add(Calendar.MONTH, 1);
				
				if ( mmIdx != mmEndIdx) {
					tableNameBuffer.append(",");
				}						
			}					
			newTableName = tableNameBuffer.toString();

		} else if ( suffixFormat.equals("YYYYMMDD") ) {
			
			//读取参数
			int yyyy = Integer.parseInt( params[1] );
			int mm = Integer.parseInt( params[2] );
			int dd =  Integer.parseInt( params[3] );
			int ddEndIdx =  Integer.parseInt( params[4] );
			
			//日期处理
			SimpleDateFormat yyyyMMddSDF = new SimpleDateFormat("yyyyMMdd"); 
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.YEAR, yyyy );
			cal.set(Calendar.MONTH, mm - 1 );
			cal.set(Calendar.DATE, dd );
			
			//表名改写
			StringBuffer tableNameBuffer = new StringBuffer();
			for(int ddIdx = 0; ddIdx <= ddEndIdx; ddIdx++) {					
				tableNameBuffer.append( tableNameElement );
				tableNameBuffer.append( yyyyMMddSDF.format(cal.getTime()) );
				
				cal.add(Calendar.DATE, 1);	
				
				if ( ddIdx != ddEndIdx) {
					tableNameBuffer.append(",");
				}
			}					
			newTableName = tableNameBuffer.toString();
		}				
		return newTableName;		
	}
	

	private Map<String, TableConfig> loadTables(Element node) {
		
		// Map<String, TableConfig> tables = new HashMap<String, TableConfig>();
		
		// 支持表名中包含引号[`] BEN GONG
		Map<String, TableConfig> tables = new TableConfigMap();
		NodeList nodeList = node.getElementsByTagName("table");
		for (int i = 0; i < nodeList.getLength(); i++) {
			Element tableElement = (Element) nodeList.item(i);
			String tableNameElement = tableElement.getAttribute("name").toUpperCase();

			//TODO:路由, 增加对动态日期表的支持
			String tableNameSuffixElement = tableElement.getAttribute("nameSuffix").toUpperCase();
			if ( !"".equals( tableNameSuffixElement ) ) {				
				
				if( tableNameElement.split(",").length > 1 ) {
					throw new ConfigException("nameSuffix " + tableNameSuffixElement + ", require name parameter cannot multiple breaks!");
				}
				//前缀用来标明日期格式
				tableNameElement = doTableNameSuffix(tableNameElement, tableNameSuffixElement);
			}
			//记录主键，用于之后路由分析，以及启用自增长主键
			String[] tableNames = tableNameElement.split(",");
			String primaryKey = tableElement.hasAttribute("primaryKey") ? tableElement.getAttribute("primaryKey").toUpperCase() : null;
			//记录是否主键自增，默认不是，（启用全局sequence handler）
			boolean autoIncrement = false;
			if (tableElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(tableElement.getAttribute("autoIncrement"));
			}
			//记录是否需要加返回结果集限制，默认需要加
			boolean needAddLimit = true;
			if (tableElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(tableElement.getAttribute("needAddLimit"));
			}
			//记录type，是否为global
			String tableTypeStr = tableElement.hasAttribute("type") ? tableElement.getAttribute("type") : null;
			int tableType = TableConfig.TYPE_GLOBAL_DEFAULT;
			if ("global".equalsIgnoreCase(tableTypeStr)) {
				tableType = TableConfig.TYPE_GLOBAL_TABLE;
			}
			//记录dataNode，就是分布在哪些dataNode上
			String dataNode = tableElement.getAttribute("dataNode");
			TableRuleConfig tableRule = null;
			if (tableElement.hasAttribute("rule")) {
				String ruleName = tableElement.getAttribute("rule");
				tableRule = tableRules.get(ruleName);
				if (tableRule == null) {
					throw new ConfigException("rule " + ruleName + " is not found!");
				}
			}
			
			boolean ruleRequired = false;
			//记录是否绑定有分片规则
			if (tableElement.hasAttribute("ruleRequired")) {
				ruleRequired = Boolean.parseBoolean(tableElement.getAttribute("ruleRequired"));
			}

			if (tableNames == null) {
				throw new ConfigException("table name is not found!");
			}
			//distribute函数，重新编排dataNode
			String distPrex = "distribute(";
			boolean distTableDns = dataNode.startsWith(distPrex);
			if (distTableDns) {
				dataNode = dataNode.substring(distPrex.length(), dataNode.length() - 1);
			}
			//分表功能
			String subTables = tableElement.getAttribute("subTables");
			
			for (int j = 0; j < tableNames.length; j++) {

				String tableName = tableNames[j];
				TableRuleConfig	tableRuleConfig=tableRule ;
				  if(tableRuleConfig!=null) {
				  	//对于实现TableRuleAware的function进行特殊处理  根据每个表新建个实例
					  RuleConfig rule= tableRuleConfig.getRule();
					  if(rule.getRuleAlgorithm() instanceof TableRuleAware)  {
						  tableRuleConfig = (TableRuleConfig) ObjectUtil.copyObject(tableRuleConfig);
						  tableRules.remove(tableRuleConfig.getName())   ;
						  String newRuleName = tableRuleConfig.getName() + "_" + tableName;
						  tableRuleConfig. setName(newRuleName);
						  TableRuleAware tableRuleAware= (TableRuleAware) tableRuleConfig.getRule().getRuleAlgorithm();
						  tableRuleAware.setRuleName(newRuleName);
						  tableRuleAware.setTableName(tableName);
						  tableRuleConfig.getRule().getRuleAlgorithm().init();
						  tableRules.put(newRuleName,tableRuleConfig);
					  }
				  }

				TableConfig table = new TableConfig(tableName, primaryKey,
						autoIncrement, needAddLimit, tableType, dataNode,
						getDbType(dataNode),
						(tableRuleConfig != null) ? tableRuleConfig.getRule() : null,
						ruleRequired, null, false, null, null,subTables);
				
				checkDataNodeExists(table.getDataNodes());
				// 检查分片表分片规则配置是否合法
				if(table.getRule() != null) {
					checkRuleSuitTable(table);
				}
				
				if (distTableDns) {
					distributeDataNodes(table.getDataNodes());
				}
				//检查去重
				if (tables.containsKey(table.getName())) {
					throw new ConfigException("table " + tableName + " duplicated!");
				}
				//放入map
				tables.put(table.getName(), table);
			}
			//只有tableName配置的是单个表（没有逗号）的时候才能有子表
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
	 *	将每个host上的datanode按照host重新排列。比如上面的例子host1拥有dn1,dn2，host2拥有dn100，dn101，host3拥有dn300，dn301,
	 * 按照host重新排列： 0->dn1 (host1),1->dn100(host2),2->dn300(host3),3->dn2(host1),4->dn101(host2),5->dn301(host3)
	 *
	 * @param theDataNodes
	 */
	private void distributeDataNodes(ArrayList<String> theDataNodes) {
		Map<String, ArrayList<String>> newDataNodeMap = new HashMap<String, ArrayList<String>>(dataHosts.size());
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
			//读取子表信息
			String cdTbName = childTbElement.getAttribute("name").toUpperCase();
			String primaryKey = childTbElement.hasAttribute("primaryKey") ? childTbElement.getAttribute("primaryKey").toUpperCase() : null;

			boolean autoIncrement = false;
			if (childTbElement.hasAttribute("autoIncrement")) {
				autoIncrement = Boolean.parseBoolean(childTbElement.getAttribute("autoIncrement"));
			}
			boolean needAddLimit = true;
			if (childTbElement.hasAttribute("needAddLimit")) {
				needAddLimit = Boolean.parseBoolean(childTbElement.getAttribute("needAddLimit"));
			}
			String subTables = childTbElement.getAttribute("subTables");
			//子表join键，和对应的parent的键，父子表通过这个关联
			String joinKey = childTbElement.getAttribute("joinKey").toUpperCase();
			String parentKey = childTbElement.getAttribute("parentKey").toUpperCase();
			TableConfig table = new TableConfig(cdTbName, primaryKey,
					autoIncrement, needAddLimit,
					TableConfig.TYPE_GLOBAL_DEFAULT, dataNodes,
					getDbType(dataNodes), null, false, parentTable, true,
					joinKey, parentKey, subTables);
			
			if (tables.containsKey(table.getName())) {
				throw new ConfigException("table " + table.getName() + " duplicated!");
			}
			tables.put(table.getName(), table);
			//对于子表的子表，递归处理
			processChildTables(tables, table, dataNodes, childTbElement);
		}
	}

	private void checkDataNodeExists(Collection<String> nodes) {
		if (nodes == null || nodes.size() < 1) {
			return;
		}
		for (String node : nodes) {
			if (!dataNodes.containsKey(node)) {
				throw new ConfigException("dataNode '" + node + "' is not found!");
			}
		}
	}
	
	/**
	 * 检查分片表分片规则配置, 目前主要检查分片表分片算法定义与分片dataNode是否匹配<br>
	 * 例如分片表定义如下:<br>
	 * {@code
	 * <table name="hotnews" primaryKey="ID" autoIncrement="true" dataNode="dn1,dn2"
			   rule="mod-long" />
	 * }
	 * <br>
	 * 分片算法如下:<br>
	 * {@code
	 * <function name="mod-long" class="io.mycat.route.function.PartitionByMod">
		<!-- how many data nodes -->
		<property name="count">3</property>
	   </function>
	 * }
	 * <br>
	 * shard table datanode(2) < function count(3) 此时检测为不匹配
	 */
	private void checkRuleSuitTable(TableConfig tableConf) {
		AbstractPartitionAlgorithm function = tableConf.getRule().getRuleAlgorithm();
		int suitValue = function.suitableFor(tableConf);
		switch(suitValue) {
			case -1:
				// 少节点,给提示并抛异常
				throw new ConfigException("Illegal table conf : table [ " + tableConf.getName() + " ] rule function [ "
						+ tableConf.getRule().getFunctionName() + " ] partition size : " + tableConf.getRule().getRuleAlgorithm().getPartitionNum() + " > table datanode size : "
						+ tableConf.getDataNodes().size() + ", please make sure table datanode size = function partition size");
			case 0:
				// table datanode size == rule function partition size
				break;
			case 1:
				// 有些节点是多余的,给出warn log
				LOGGER.warn("table conf : table [ {} ] rule function [ {} ] partition size : {} < table datanode size : {} , this cause some datanode to be redundant", 
						new String[]{
								tableConf.getName(),
								tableConf.getRule().getFunctionName(),
								String.valueOf(tableConf.getRule().getRuleAlgorithm().getPartitionNum()),
								String.valueOf(tableConf.getDataNodes().size())
						});
				break;
		}
	}

	private void loadDataNodes(Element root) {
		//读取DataNode分支
		NodeList list = root.getElementsByTagName("dataNode");
		for (int i = 0, n = list.getLength(); i < n; i++) {
			Element element = (Element) list.item(i);
			String dnNamePre = element.getAttribute("name");

			String databaseStr = element.getAttribute("database");
			String host = element.getAttribute("dataHost");
			//字符串不为空
			if (empty(dnNamePre) || empty(databaseStr) || empty(host)) {
				throw new ConfigException("dataNode " + dnNamePre + " define error ,attribute can't be empty");
			}
			//dnNames（name）,databases（database）,hostStrings（dataHost）都可以配置多个，以',', '$', '-'区分，但是需要保证database的个数*dataHost的个数=name的个数
			//多个dataHost与多个database如果写在一个标签，则每个dataHost拥有所有database
			//例如：<dataNode name="dn1$0-75" dataHost="localhost$1-10" database="db$0-759" />
			//则为：localhost1拥有dn1$0-75,localhost2也拥有dn1$0-75（对应db$76-151）
			String[] dnNames = io.mycat.util.SplitUtil.split(dnNamePre, ',', '$', '-');
			String[] databases = io.mycat.util.SplitUtil.split(databaseStr, ',', '$', '-');
			String[] hostStrings = io.mycat.util.SplitUtil.split(host, ',', '$', '-');

			if (dnNames.length > 1 && dnNames.length != databases.length * hostStrings.length) {
				throw new ConfigException("dataNode " + dnNamePre
								+ " define error ,dnNames.length must be=databases.length*hostStrings.length");
			}
			if (dnNames.length > 1) {
				
				List<String[]> mhdList = mergerHostDatabase(hostStrings, databases);
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

	/**
	 * 匹配DataHost和Database，每个DataHost拥有每个Database名字
	 * @param hostStrings
	 * @param databases
     * @return
     */
	private List<String[]> mergerHostDatabase(String[] hostStrings,	String[] databases) {
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
			throw new ConfigException("dataNode " + conf.getName() + " duplicated!");
		}
		
		if (!dataHosts.containsKey(host)) {
			throw new ConfigException("dataNode " + dnName + " reference dataHost:" + host + " not exists!");
		}

		dataHosts.get(host).addDataNode(conf.getName());
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
		String usingDecrypt = node.getAttribute("usingDecrypt");
		String passwordEncryty= DecryptUtil.DBHostDecrypt(usingDecrypt, nodeHost, user, password);
		
		String weightStr = node.getAttribute("weight");
		int weight = "".equals(weightStr) ? PhysicalDBPool.WEIGHT : Integer.parseInt(weightStr) ;
		
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
				throw new ConfigException("invalid jdbc url " + nodeUrl + " of " + dataHost);
			}
			ip = url.getHost();
			port = url.getPort();
		}

		DBHostConfig conf = new DBHostConfig(nodeHost, ip, port, nodeUrl, user, passwordEncryty,password);
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
			//判断是否重复
			if (dataHosts.containsKey(name)) {
				throw new ConfigException("dataHost name " + name + "duplicated!");
			}
			//读取最大连接数
			int maxCon = Integer.parseInt(element.getAttribute("maxCon"));
			//读取最小连接数
			int minCon = Integer.parseInt(element.getAttribute("minCon"));
			/**
			 * 读取负载均衡配置
			 * 1. balance="0", 不开启分离机制，所有读操作都发送到当前可用的 writeHost 上。
			 * 2. balance="1"，全部的 readHost 和 stand by writeHost 参不 select 的负载均衡
			 * 3. balance="2"，所有读操作都随机的在 writeHost、readhost 上分发。
			 * 4. balance="3"，所有读请求随机的分发到 wiriterHost 对应的 readhost 执行，writerHost 不负担读压力
			 */
			int balance = Integer.parseInt(element.getAttribute("balance"));
			/**
			 * 读取切换类型
			 * -1 表示不自动切换
			 * 1 默认值，自动切换
			 * 2 基于MySQL主从同步的状态决定是否切换
			 * 心跳询句为 show slave status
			 * 3 基于 MySQL galary cluster 的切换机制
			 */
			String switchTypeStr = element.getAttribute("switchType");
			int switchType = switchTypeStr.equals("") ? -1 : Integer.parseInt(switchTypeStr);
			//读取从延迟界限
			String slaveThresholdStr = element.getAttribute("slaveThreshold");
			int slaveThreshold = slaveThresholdStr.equals("") ? -1 : Integer.parseInt(slaveThresholdStr);
			
			//如果 tempReadHostAvailable 设置大于 0 则表示写主机如果挂掉， 临时的读服务依然可用
			String tempReadHostAvailableStr = element.getAttribute("tempReadHostAvailable");
			boolean tempReadHostAvailable = !tempReadHostAvailableStr.equals("") && Integer.parseInt(tempReadHostAvailableStr) > 0;
			/**
			 * 读取 写类型
			 * 这里只支持 0 - 所有写操作仅配置的第一个 writeHost
			 */
			String writeTypStr = element.getAttribute("writeType");
			int writeType = "".equals(writeTypStr) ? PhysicalDBPool.WRITE_ONLYONE_NODE : Integer.parseInt(writeTypStr);


			String dbDriver = element.getAttribute("dbDriver");
			String dbType = element.getAttribute("dbType");
			String filters = element.getAttribute("filters");
			String logTimeStr = element.getAttribute("logTime");
			String slaveIDs = element.getAttribute("slaveIDs");
			long logTime = "".equals(logTimeStr) ? PhysicalDBPool.LONG_TIME : Long.parseLong(logTimeStr) ;
			//读取心跳语句
			String heartbeatSQL = element.getElementsByTagName("heartbeat").item(0).getTextContent();
			//读取 初始化sql配置,用于oracle
			NodeList connectionInitSqlList = element.getElementsByTagName("connectionInitSql");
			String initConSQL = null;
			if (connectionInitSqlList.getLength() > 0) {
				initConSQL = connectionInitSqlList.item(0).getTextContent();
			}
			//读取writeHost
			NodeList writeNodes = element.getElementsByTagName("writeHost");
			DBHostConfig[] writeDbConfs = new DBHostConfig[writeNodes.getLength()];
			Map<Integer, DBHostConfig[]> readHostsMap = new HashMap<Integer, DBHostConfig[]>(2);
			Set<String> writeHostNameSet = new HashSet<String>(writeNodes.getLength());
			for (int w = 0; w < writeDbConfs.length; w++) {
				Element writeNode = (Element) writeNodes.item(w);
				writeDbConfs[w] = createDBHostConf(name, writeNode, dbType, dbDriver, maxCon, minCon,filters,logTime);
				if(writeHostNameSet.contains(writeDbConfs[w].getHostName())) {
					throw new ConfigException("writeHost " + writeDbConfs[w].getHostName() + " duplicated!");
				} else {
					writeHostNameSet.add(writeDbConfs[w].getHostName());
				}
				NodeList readNodes = writeNode.getElementsByTagName("readHost");
				//读取对应的每一个readHost
				if (readNodes.getLength() != 0) {
					DBHostConfig[] readDbConfs = new DBHostConfig[readNodes.getLength()];
					Set<String> readHostNameSet = new HashSet<String>(readNodes.getLength());
					for (int r = 0; r < readDbConfs.length; r++) {
						Element readNode = (Element) readNodes.item(r);
						readDbConfs[r] = createDBHostConf(name, readNode, dbType, dbDriver, maxCon, minCon,filters, logTime);
						if(readHostNameSet.contains(readDbConfs[r].getHostName())) {
							throw new ConfigException("readHost " + readDbConfs[r].getHostName() + " duplicated!");
						} else {
							readHostNameSet.add(readDbConfs[r].getHostName());
						}
					}
					readHostsMap.put(w, readDbConfs);
				}
			}

			DataHostConfig hostConf = new DataHostConfig(name, dbType, dbDriver, 
					writeDbConfs, readHostsMap, switchType, slaveThreshold, tempReadHostAvailable);		
			
			hostConf.setMaxCon(maxCon);
			hostConf.setMinCon(minCon);
			hostConf.setBalance(balance);
			hostConf.setWriteType(writeType);
			hostConf.setHearbeatSQL(heartbeatSQL);
			hostConf.setConnectionInitSql(initConSQL);
			hostConf.setFilters(filters);
			hostConf.setLogTime(logTime);
			hostConf.setSlaveIDs(slaveIDs);
			dataHosts.put(hostConf.getName(), hostConf);
		}
	}

}
