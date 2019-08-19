package io.mycat.util.dataMigrator;

import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.*;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.config.util.ConfigException;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

/**
 * 数据迁移新旧配置文件加载、对比
 * @author haonan108
 *
 */
public class ConfigComparer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ConfigComparer.class);
	/*
	 *指定需要进行数据迁移的表及对应schema
	 * 配置文件格式
	 * schema1=tb1,tb2,...
	 * schema2=all
	 * ...
	 */
	private final static String TABLES_FILE = "/migrateTables.properties";  
    private final static String NEW_SCHEMA = "/newSchema.xml";
	private final static String NEW_RULE = "/newRule.xml";
	private final static String DN_INDEX_FILE = "/dnindex.properties";
	
	private SchemaLoader oldLoader;
	private SchemaLoader newLoader;
	
	private  Map<String, DataHostConfig>  oldDataHosts;
	private  Map<String, DataNodeConfig>  oldDataNodes;
	private  Map<String, SchemaConfig>  oldSchemas; 
	
	private  Map<String, DataHostConfig> newDataHosts;
	private  Map<String, DataNodeConfig> newDataNodes;
	private  Map<String, SchemaConfig> newSchemas;
	
	//即使发生主备切换也使用主数据源
	private boolean isAwaysUseMaster;
	private Properties dnIndexProps;
	
	//此类主要目的是通过加载新旧配置文件来获取表迁移信息，migratorTables就是最终要获取的迁移信息集合
	private List<TableMigrateInfo> migratorTables = new ArrayList<TableMigrateInfo>();
	
	public ConfigComparer(boolean isAwaysUseMaster) throws Exception{
		this.isAwaysUseMaster = isAwaysUseMaster;
		loadOldConfig();
		loadNewConfig();
		loadTablesFile();
	}
	
	public List<TableMigrateInfo> getMigratorTables(){
		return migratorTables;
	}
	
	private void loadOldConfig(){
		try{
			oldLoader = new XMLSchemaLoader();
			oldDataHosts = oldLoader.getDataHosts();
			oldDataNodes = oldLoader.getDataNodes();
			oldSchemas = oldLoader.getSchemas();
		}catch(Exception e){
			throw new ConfigException(" old config for migrate read fail!please check schema.xml or  rule.xml  "+e);
		}
		
	}
	
	private void loadNewConfig(){
		try{
			newLoader = new XMLSchemaLoader(NEW_SCHEMA, NEW_RULE);
			newDataHosts = newLoader.getDataHosts();
			newDataNodes = newLoader.getDataNodes();
			newSchemas = newLoader.getSchemas();
		}catch(Exception e){
			throw new ConfigException(" new config for migrate read fail!please check newSchema.xml or  newRule.xml  "+e);
		}
		
	}
	
	
	private void loadTablesFile() throws Exception{
		Properties pro = new Properties();
		if(!isAwaysUseMaster){
			dnIndexProps = loadDnIndexProps();
		}
		try{
			pro.load(ConfigComparer.class.getResourceAsStream(TABLES_FILE));
		}catch(Exception e){
			throw new ConfigException("tablesFile.properties read fail!");
		}
		Iterator<Entry<Object, Object>> it = pro.entrySet().iterator();
		while(it.hasNext()){
			Entry<Object, Object> entry  = it.next();
			String schemaName = entry.getKey().toString();
			String tables = entry.getValue().toString();
			loadMigratorTables(schemaName,getTables(tables));
		}
	}
	
	private String[] getTables(String tables){
		if(tables.equalsIgnoreCase("all") || tables.isEmpty()){
			return new String[]{};
		}else{
			return tables.split(",");
		}
	}
	
	/*
	 * 加载迁移表信息，tables大小为0表示迁移schema下所有表
	 */
	private void loadMigratorTables(String schemaName,String[] tables){
		if(!DataMigratorUtil.isKeyExistIgnoreCase(oldSchemas, schemaName)){
			throw new ConfigException("oldSchema:"+schemaName+" is not exists!");
		}
		if(!DataMigratorUtil.isKeyExistIgnoreCase(newSchemas,schemaName)){
			throw new ConfigException("newSchema:"+schemaName+" is not exists!");
		}
		Map<String, TableConfig> oldTables =  DataMigratorUtil.getValueIgnoreCase(oldSchemas, schemaName).getTables();
		Map<String, TableConfig> newTables = DataMigratorUtil.getValueIgnoreCase(newSchemas, schemaName).getTables();
		if(tables.length>0){
			//指定schema下的表进行迁移
			for(int i =0;i<tables.length;i++){
				TableConfig oldTable =  DataMigratorUtil.getValueIgnoreCase(oldTables,tables[i]);
				TableConfig newTable = DataMigratorUtil.getValueIgnoreCase(newTables,tables[i]);
				loadMigratorTable(oldTable, newTable,schemaName,tables[i]);
			}
		}else{
			//迁移schema下所有的表
			//校验新旧schema中的table配置是否一致
			Set<String> oldSet = oldTables.keySet();
			Set<String> newSet = newTables.keySet();
			if(!oldSet.equals(newSet)){
				throw new ConfigException("new & old table config is not equal!");
			}
			for(String tableName:oldSet){
				TableConfig oldTable = oldTables.get(tableName);
				TableConfig newTable = newTables.get(tableName);
				loadMigratorTable(oldTable, newTable,schemaName,tableName);
			}
		}
		
	}
	
	
	
	private void loadMigratorTable(TableConfig oldTable,TableConfig newTable,String schemaName,String tableName){
		//禁止配置非拆分表
		if(oldTable == null || newTable == null){
			throw new ConfigException("please check tableFile.properties,make sure "+schemaName+":"+tableName+" is sharding table ");
		}
		//忽略全局表
		if(oldTable.isGlobalTable()||newTable.isGlobalTable()){
			String message = "global table: "+schemaName+":"+tableName+" is ignore!";
			System.out.println("Warn: "+message);
			LOGGER.warn(message);
		}else{
			List<DataNode > oldDN = getDataNodes(oldTable,oldDataNodes,oldDataHosts);
			List<DataNode > newDN = getDataNodes(newTable,newDataNodes,newDataHosts);
			//忽略数据节点分布没有发生变化的表
			if(isNeedMigrate(oldDN,newDN)){
				checkRuleConfig(oldTable.getRule(), newTable.getRule(),schemaName,tableName);
				RuleConfig newRC=newTable.getRule();
				TableMigrateInfo tmi = new TableMigrateInfo(schemaName, tableName, oldDN, newDN, newRC.getRuleAlgorithm(), newRC.getColumn());
				migratorTables.add(tmi);
			}else{
				String message = schemaName+":"+tableName+" is ignore,no need to migrate!";
				LOGGER.warn(message);
				System.out.println("Warn: "+message);
			}
			
		}
	}
	
	//对比前后表数据节点分布是否一致
	private boolean isNeedMigrate(List<DataNode> oldDN,List<DataNode> newDN){
		if(oldDN.size() != newDN.size()){
			return true;
		}
		return false;
	}
	
	//获取拆分表对应节点列表,具体到实例地址、库
	private List<DataNode> getDataNodes(TableConfig tableConfig,Map<String, DataNodeConfig> dnConfig,Map<String, DataHostConfig> dhConfig){
		List<DataNode> dataNodes = new ArrayList<DataNode>();
		//TO-DO
		ArrayList<String> dataNodeNames = tableConfig.getDataNodes();
		int i = 0;
		for(String name:dataNodeNames){
			DataNodeConfig config = dnConfig.get(name);
			String db = config.getDatabase();
			String dataHost = config.getDataHost();
			DataHostConfig dh = dhConfig.get(dataHost);
			String dbType = dh.getDbType();
			DBHostConfig[]  writeHosts = dh.getWriteHosts();
			DBHostConfig currentWriteHost;
			if(isAwaysUseMaster){
				currentWriteHost = writeHosts[0];
			}else{
			    //迁移数据发生在当前切换后的数据源
				currentWriteHost = writeHosts[Integer.valueOf(dnIndexProps.getProperty(dh.getName()))];
			}
			DataNode dn = new DataNode(name,currentWriteHost.getIp(), currentWriteHost.getPort(), currentWriteHost.getUser(), currentWriteHost.getPassword(), db, dbType,i++);
			dataNodes.add(dn);
		}
		
		return dataNodes;
	}
	
	//校验前后路由规则是否一致
	private void checkRuleConfig(RuleConfig oldRC,RuleConfig newRC,String schemaName,String tableName){
		if(!oldRC.getColumn().equalsIgnoreCase(newRC.getColumn())){
			throw new ConfigException(schemaName+":"+tableName+" old & new partition column is not same!");
		}
		AbstractPartitionAlgorithm oldAlg = oldRC.getRuleAlgorithm();
		AbstractPartitionAlgorithm newAlg = newRC.getRuleAlgorithm();
		//判断路由算法前后是否一致
		if(!oldAlg.getClass().isAssignableFrom(newAlg.getClass())){
			throw new ConfigException(schemaName+":"+tableName+" old & new rule Algorithm is not same!");
		}
	}
	
	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		InputStream is = null;
		try {
			is = ConfigComparer.class.getResourceAsStream(DN_INDEX_FILE);
			prop.load(is);
		} catch (Exception e) {
			throw new ConfigException("please check file \"dnindex.properties\" "+e.getMessage());
		} finally {
			try {
				if(is !=null){
					is.close();
				}
			} catch (IOException e) {
				throw new ConfigException(e.getMessage());
			}
		}
		return prop;
	}
}
