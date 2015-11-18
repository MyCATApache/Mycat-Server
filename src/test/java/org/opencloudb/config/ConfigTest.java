package org.opencloudb.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.loader.ConfigLoader;
import org.opencloudb.config.loader.xml.XMLConfigLoader;
import org.opencloudb.config.loader.xml.XMLSchemaLoader;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.jdbc.JDBCDatasource;
import org.opencloudb.mysql.nio.MySQLDataSource;

import junit.framework.Assert;

public class ConfigTest {
	
	private SystemConfig system;
	private final Map<String, UserConfig> users;
	private Map<String, SchemaConfig> schemas;
	private Map<String, PhysicalDBPool> dataHosts;	
	
	public ConfigTest() {
		
		String schemaFile = "/config/schema.xml";
		String ruleFile = "/config/rule.xml";
		
		XMLSchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		XMLConfigLoader configLoader = new XMLConfigLoader(schemaLoader);
		
		this.system = configLoader.getSystemConfig();
		this.users = configLoader.getUserConfigs();
		this.schemas = configLoader.getSchemaConfigs();		
        this.dataHosts = initDataHosts(configLoader);
        
	}
	
	/**
	 * 测试 临时读可用 配置
	 */
	@Test
	public void testTempReadHostAvailable() {
		PhysicalDBPool pool = this.dataHosts.get("localhost2");   
		DataHostConfig hostConfig = pool.getSource().getHostConfig();
		Assert.assertTrue( hostConfig.isTempReadHostAvailable() == true );
	}
	
	/**
	 * 测试 用户服务降级 拒连 配置
	 */
	@Test
	public void testReadUserBenchmark() {
		UserConfig userConfig = this.users.get("test");
		int benchmark = userConfig.getBenchmark();
		Assert.assertTrue( benchmark == 11111 );
	}
	
	
	/**
     * 测试 读服务的 权重
     *
     * @throws Exception
     */
    @Test
    public void testReadHostWeight() throws Exception {
    	
    	ArrayList<PhysicalDatasource> okSources = new ArrayList<PhysicalDatasource>();
    	
    	PhysicalDBPool pool = this.dataHosts.get("localhost2");   
    	okSources.addAll(pool.getAllDataSources());    	
    	PhysicalDatasource source = pool.randomSelect( okSources );
  
    	Assert.assertTrue( source != null );
    }
    
    /**
     * 测试 动态日期表
     *
     * @throws Exception
     */
    @Test
    public void testDynamicYYYYMMTable() throws Exception {
    	SchemaConfig sc = this.schemas.get("dbtest1");
    	Map<String, TableConfig> tbm = sc.getTables();
    	Assert.assertTrue( tbm.size() == 32);    	
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
		PhysicalDBPool pool = new PhysicalDBPool(conf.getName(),conf, writeSources,
				readSourcesMap, conf.getBalance(), conf.getWriteType());
		return pool;
	}

}
