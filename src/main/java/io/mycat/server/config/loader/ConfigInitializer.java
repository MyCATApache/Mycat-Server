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

import io.mycat.backend.MySQLDataSource;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.jdbc.JDBCDatasource;
import io.mycat.backend.postgresql.PostgreSQLDataSource;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.node.CharsetConfig;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.HostIndexConfig;
import io.mycat.server.config.node.QuarantineConfig;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SequenceConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.UserConfig;
import io.mycat.server.packet.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author mycat
 */
public class ConfigInitializer {
	private volatile SystemConfig system;
	private volatile MycatClusterConfig cluster;
	private volatile QuarantineConfig quarantine;
	private volatile Map<String, UserConfig> users;
	private volatile Map<String, SchemaConfig> schemas;
	private volatile Map<String, PhysicalDBNode> dataNodes;
	private volatile Map<String, PhysicalDBPool> dataHosts;
	private volatile Map<String, RuleConfig> tableRules;
	private volatile SequenceConfig sequenceConfig;
	private volatile CharsetConfig charsetConfig;

	public ConfigInitializer(boolean loadDataHost) {
		ConfigLoader configLoader = ConfigFactory.instanceLoader();

		this.system = configLoader.getSystemConfig();
		this.users = configLoader.getUserConfigs();
		if (loadDataHost) {
			this.dataHosts = initDataHosts(configLoader);
			this.dataNodes = initDataNodes(configLoader);
		}
		this.initCharsetConfig(configLoader);	// 需要放在 initDataHosts 后面
		this.tableRules = configLoader.getTableRuleConfigs();
		this.schemas = configLoader.getSchemaConfigs();
		this.quarantine = configLoader.getQuarantineConfigs();
		this.cluster = configLoader.getClusterConfigs();
		this.sequenceConfig = configLoader.getSequenceConfig();
		this.charsetConfig = new CharsetConfig();

		this.checkConfig();
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

	public MycatClusterConfig getCluster() {
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

	public CharsetConfig getCharsetConfig() {
		return this.charsetConfig;
	}

	public SequenceConfig getSequenceConfig() {
		return this.sequenceConfig;
	}

	/*
	 * private MycatCluster initCobarCluster(ConfigLoader configLoader) { return
	 * new MycatCluster(configLoader.getClusterConfigs()); }
	 */
	public Map<String, RuleConfig> getTableRules() {
		return tableRules;
	}

	public void setTableRules(Map<String, RuleConfig> tableRules) {
		this.tableRules = tableRules;
	}

	private Map<String, PhysicalDBPool> initDataHosts(ConfigLoader configLoader) {
		Map<String, DataHostConfig> nodeConfs = configLoader
				.getDataHostConfigs();
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

		} else if (dbDriver.equals("jdbc")) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				JDBCDatasource ds = new JDBCDatasource(nodes[i], conf, isRead);
				dataSources[i] = ds;
			}
		} else if ("PostgreSQL".equalsIgnoreCase(dbType)
				&& "native".equals(dbDriver)) {
			for (int i = 0; i < nodes.length; i++) {
				nodes[i].setIdleTimeout(system.getIdleTimeout());
				PostgreSQLDataSource ds = new PostgreSQLDataSource(nodes[i],
						conf, isRead);
				dataSources[i] = ds;
			}
		} else {
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
		return new PhysicalDBPool(conf.getName(), conf,
				writeSources, readSourcesMap, conf.getBalance(),
				conf.getWriteType());
	}

	private Map<String, PhysicalDBNode> initDataNodes(ConfigLoader configLoader) {
		Map<String, DataNodeConfig> nodeConfs = configLoader
				.getDataNodeConfigs();
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

	private void initCharsetConfig(ConfigLoader configLoader) {
		this.charsetConfig = configLoader.getCharsetConfigs();
		CharsetUtil.load(this.dataHosts, charsetConfig.getProps());
//		CharsetUtil.asynLoad(this.dataHosts, charsetConfig.getProps());
	}

	public HostIndexConfig getHostIndexs() {
		ConfigLoader configLoader = ConfigFactory.instanceLoader();
		return configLoader.getHostIndexConfig();
	}

}