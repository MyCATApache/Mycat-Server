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
package org.opencloudb;

import java.util.*;

import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.loader.ConfigLoader;
import org.opencloudb.config.loader.SchemaLoader;
import org.opencloudb.config.loader.xml.XMLConfigLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.*;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.jdbc.JDBCDatasource;
import org.opencloudb.mysql.nio.MySQLDataSource;

/**
 * @author mycat
 */
public class ConfigInitializer {
	private volatile SystemConfig system;
	private volatile MycatCluster cluster;
	private volatile QuarantineConfig quarantine;
	private volatile Map<String, UserConfig> users;
	private volatile Map<String, SchemaConfig> schemas;
	private volatile Map<String, PhysicalDBNode> dataNodes;
	private volatile Map<String, PhysicalDBPool> dataHosts;

	public ConfigInitializer() {
		SchemaLoader schemaLoader = new XMLSchemaLoader();
		XMLConfigLoader configLoader = new XMLConfigLoader(schemaLoader);
		schemaLoader = null;
		this.system = configLoader.getSystemConfig();
		this.users = configLoader.getUserConfigs();
		this.schemas = configLoader.getSchemaConfigs();
		this.dataHosts = initDataHosts(configLoader);
		this.dataNodes = initDataNodes(configLoader);
		this.quarantine = configLoader.getQuarantineConfig();
		this.cluster = initCobarCluster(configLoader);

		this.checkConfig();
		initDbType(configLoader);
	}

	private void initDbType(XMLConfigLoader configLoader)
	{
		for (SchemaConfig sc : schemas.values()) {
			if (sc!=null) {
				Map<String, TableConfig> tableConfigMap=  sc.getTables() ;
				if(tableConfigMap==null) continue;
				for (String tableName : tableConfigMap.keySet())
				{
					TableConfig tableConfig=	tableConfigMap.get(tableName);
					sc.addTableDbType(tableName,getDbTypeFrom(tableConfig.getDataNodes(),configLoader));
				}
			}
		}
	}

	private Set<String> getDbTypeFrom(List<String> dataNodes,XMLConfigLoader configLoader)
	{
		Map<String, DataNodeConfig> dataNodeConfigMap = configLoader.getDataNodes();
		Map<String, DataHostConfig> dataHostConfigMap = configLoader.getDataHosts();
		Set<String> dbTypes=new HashSet<>();
		for (String dataNode : dataNodes)
		{
			DataNodeConfig dataNodeConfig=	 dataNodeConfigMap.get(dataNode);
			DataHostConfig host=	dataHostConfigMap.get(dataNodeConfig.getDataHost());
			dbTypes.add(host.getDbType());
		}
		 return dbTypes;
	}

	private void checkConfig() throws ConfigException {
		if (users == null || users.isEmpty())
			return;
		for (UserConfig uc : users.values()) {
			if (uc == null) {
				continue;
			}
			Set<String> authSchemas = uc.getSchemas();
			if (authSchemas == null) {
				continue;
			}
			for (String schema : authSchemas) {
				if (!schemas.containsKey(schema)) {
					String errMsg = "schema " + schema + " refered by user "
							+ uc.getName() + " is not exist!";
					throw new ConfigException(errMsg);
				}
			}
		}

		for (SchemaConfig sc : schemas.values()) {
			if (null == sc) {
				continue;
			}
		}
	}

	public SystemConfig getSystem() {
		return system;
	}

	public MycatCluster getCluster() {
		return cluster;
	}

	public QuarantineConfig getQuarantine() {
		return quarantine;
	}

	public Map<String, UserConfig> getUsers() {
		return users;
	}

	public Map<String, SchemaConfig> getSchemas() {
		return schemas;
	}

	public Map<String, PhysicalDBNode> getDataNodes() {
		return dataNodes;
	}

	public Map<String, PhysicalDBPool> getDataHosts() {
		return this.dataHosts;
	}

	private MycatCluster initCobarCluster(ConfigLoader configLoader) {
		return new MycatCluster(configLoader.getClusterConfig());
	}

	private Map<String, PhysicalDBPool> initDataHosts(ConfigLoader configLoader) {
		Map<String, DataHostConfig> nodeConfs = configLoader.getDataHosts();
		Map<String, PhysicalDBPool> nodes = new HashMap<String, PhysicalDBPool>(
				nodeConfs.size());
		for (DataHostConfig conf : nodeConfs.values()) {
			PhysicalDBPool pool = getPhysicalDBPool(conf, configLoader);
			nodes.put(pool.getHostName(), pool);
		}
		return nodes;
	}

	private PhysicalDatasource[] createDataSource(DataHostConfig conf,
			String hostName, String dbType, String dbDriver,
			DBHostConfig[] nodes, boolean isRead) {
		PhysicalDatasource[] dataSources = new PhysicalDatasource[nodes.length];
		if (dbType.equals("mysql") && dbDriver.equals("native")) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}

		} else if(dbDriver.equals("jdbc"))
			{
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}
			}
		else {
			throw new ConfigException("not supported yet !" + hostName);
		}
		return dataSources;
	}

	private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf,
			ConfigLoader configLoader) {
		String name = conf.getName();
		String dbType = conf.getDbType();
		String dbDriver = conf.getDbDriver();
		PhysicalDatasource[] writeSources = createDataSource(conf, name,
				dbType, dbDriver, conf.getWriteHosts(), false);
		Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
		Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<Integer, PhysicalDatasource[]>(
				readHostsMap.size());
		for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
			PhysicalDatasource[] readSources = createDataSource(conf, name,
					dbType, dbDriver, entry.getValue(), true);
			readSourcesMap.put(entry.getKey(), readSources);
		}
		PhysicalDBPool pool = new PhysicalDBPool(conf.getName(), writeSources,
				readSourcesMap, conf.getBalance(), conf.getWriteType());
		return pool;
	}

	private Map<String, PhysicalDBNode> initDataNodes(ConfigLoader configLoader) {
		Map<String, DataNodeConfig> nodeConfs = configLoader.getDataNodes();
		Map<String, PhysicalDBNode> nodes = new HashMap<String, PhysicalDBNode>(
				nodeConfs.size());
		for (DataNodeConfig conf : nodeConfs.values()) {
			PhysicalDBPool pool = this.dataHosts.get(conf.getDataHost());
			if (pool == null) {
				throw new ConfigException("dataHost not exists "
						+ conf.getDataHost());

			}
			PhysicalDBNode dataNode = new PhysicalDBNode(conf.getName(),
					conf.getDatabase(), pool);
			nodes.put(dataNode.getName(), dataNode);
		}
		return nodes;
	}

}