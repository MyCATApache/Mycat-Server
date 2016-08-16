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
package io.mycat.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.jdbc.JDBCDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.backend.postgresql.PostgreSQLDataSource;
import io.mycat.config.loader.ConfigLoader;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLConfigLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.DataHostConfig;
import io.mycat.config.model.DataNodeConfig;
import io.mycat.config.model.QuarantineConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.route.sequence.handler.DistributedSequenceHandler;
import io.mycat.route.sequence.handler.IncrSequenceMySQLHandler;
import io.mycat.route.sequence.handler.IncrSequenceTimeHandler;
import io.mycat.route.sequence.handler.IncrSequenceZKHandler;

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

	public ConfigInitializer(boolean loadDataHost) {
		//读取rule.xml和schema.xml
		SchemaLoader schemaLoader = new XMLSchemaLoader();
		//读取server.xml
		XMLConfigLoader configLoader = new XMLConfigLoader(schemaLoader);
		schemaLoader = null;
		//加载配置
		this.system = configLoader.getSystemConfig();
		this.users = configLoader.getUserConfigs();
		this.schemas = configLoader.getSchemaConfigs();
		//是否重新加载DataHost和对应的DataNode
		if (loadDataHost) {
			this.dataHosts = initDataHosts(configLoader);
			this.dataNodes = initDataNodes(configLoader);
		}
		//权限管理
		this.quarantine = configLoader.getQuarantineConfig();
		this.cluster = initCobarCluster(configLoader);
		//不同类型的全局序列处理器的配置加载
		if (system.getSequnceHandlerType() == SystemConfig.SEQUENCEHANDLER_MYSQLDB) {
			IncrSequenceMySQLHandler.getInstance().load();
		}
		if (system.getSequnceHandlerType() == SystemConfig.SEQUENCEHANDLER_LOCAL_TIME) {
			IncrSequenceTimeHandler.getInstance().load();
		}
		if (system.getSequnceHandlerType() == SystemConfig.SEQUENCEHANDLER_ZK_DISTRIBUTED) {
			DistributedSequenceHandler.getInstance(system).load();
		}
		if (system.getSequnceHandlerType() == SystemConfig.SEQUENCEHANDLER_ZK_GLOBAL_INCREMENT) {
			IncrSequenceZKHandler.getInstance().load();
		}
		//检查user与schema配置对应以及schema配置不为空
		this.checkConfig();
	}

	private void checkConfig() throws ConfigException {
		if (users == null || users.isEmpty()) {
			return;
		}
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
		//根据DataHost建立PhysicalDBPool，其实就是实际数据库连接池，每个DataHost对应一个PhysicalDBPool
		Map<String, PhysicalDBPool> nodes = new HashMap<String, PhysicalDBPool>(
				nodeConfs.size());
		for (DataHostConfig conf : nodeConfs.values()) {
			//建立PhysicalDBPool
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
				//设置最大idle时间，默认为30分钟
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				MySQLDataSource ds = new MySQLDataSource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}

		} else if (dbDriver.equals("jdbc")) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}
		} else if ("postgresql".equalsIgnoreCase(dbType) && dbDriver.equalsIgnoreCase("native")){
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				PostgreSQLDataSource ds = new PostgreSQLDataSource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}
		} else{
			throw new ConfigException("not supported yet !" + hostName);
		}
		return dataSources;
	}

	private PhysicalDBPool getPhysicalDBPool(DataHostConfig conf,
			ConfigLoader configLoader) {
		String name = conf.getName();
		//数据库类型，我们这里只讨论MySQL
		String dbType = conf.getDbType();
		//连接数据库驱动，我们这里只讨论MyCat自己实现的native
		String dbDriver = conf.getDbDriver();
		//针对所有写节点创建PhysicalDatasource
		PhysicalDatasource[] writeSources = createDataSource(conf, name,
				dbType, dbDriver, conf.getWriteHosts(), false);
		Map<Integer, DBHostConfig[]> readHostsMap = conf.getReadHosts();
		Map<Integer, PhysicalDatasource[]> readSourcesMap = new HashMap<Integer, PhysicalDatasource[]>(
				readHostsMap.size());
		//对于每个读节点建立key为writeHost下标value为readHost的PhysicalDatasource[]的哈希表
		for (Map.Entry<Integer, DBHostConfig[]> entry : readHostsMap.entrySet()) {
			PhysicalDatasource[] readSources = createDataSource(conf, name,
					dbType, dbDriver, entry.getValue(), true);
			readSourcesMap.put(entry.getKey(), readSources);
		}
		PhysicalDBPool pool = new PhysicalDBPool(conf.getName(), conf,
				writeSources, readSourcesMap, conf.getBalance(),
				conf.getWriteType());
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