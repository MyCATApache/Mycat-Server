package io.mycat.util.dataMigrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * 数据迁移统一调度类，支持扩容缩容
 * 原理：读取需要迁移的数据节点表所有拆分字段数据，按照扩容或缩容后的配置对拆分字段重新计算路由节点，
 * 将需要迁移的数据导出，然后导入到扩容或缩容后对应的数据节点
 * @author haonan108
 *
 */
public class DataMigrator {
 
	private static final Logger LOGGER = LoggerFactory.getLogger(DataMigrator.class);
	
	public static  DataMigratorArgs margs;
	
	private List<TableMigrateInfo> migrateTables;
	
	private ExecutorService executor;
	
	private List<DataNodeClearGroup> clearGroup = new ArrayList<>();
	
	public DataMigrator(String[] args){
		margs = new DataMigratorArgs(args);
		executor = new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),new ThreadPoolExecutor.CallerRunsPolicy());
		
		
		try {
			createTempParentDir(margs.getTempFileDir());
			ConfigComparer loader = new ConfigComparer(margs.isAwaysUseMaster());
			migrateTables = loader.getMigratorTables();
			//建表
			for(TableMigrateInfo table:migrateTables){
				table.setTableStructure();
				table.createTableToNewDataNodes();
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage(),e);
			System.out.println(e.getMessage());
			//配置错误退出迁移程序
			System.exit(-1);
		}
	}
	
	public static void main(String[] args) throws SQLException {
		long start = System.currentTimeMillis();
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
		System.out.println("\n"+format.format(new Date())+" [1]-> creating migrator schedule and temp files for migrate...");
		//初始化配置
		DataMigrator migrator = new DataMigrator(args);
		
		//生成中间文件
		migrator.createTempFiles();
		migrator.changeSize();
		migrator.printInfo();

		//迁移数据
		System.out.println("\n"+format.format(new Date())+" [2]-> start migrate data...");
		migrator.migrateData();
		
		//清除中间临时文件、清除被迁移掉的冗余数据
		System.out.println("\n"+format.format(new Date())+" [3]-> cleaning redundant data...");
		migrator.clear();
		
		//校验数据是否迁移成功
		System.out.println("\n"+format.format(new Date())+" [4]-> validating tables migrate result...");
		migrator.validate();
		migrator.clearTempFiles();
		long end = System.currentTimeMillis();
		System.out.println("\n"+format.format(new Date())+" migrate data complete in "+(end-start)+"ms");
	}
	
	//打印各个表的迁移数据信息
	private void printInfo() {
		for(TableMigrateInfo table:migrateTables){
			table.printMigrateInfo();
			table.printMigrateSchedule();
		}
	}

	//删除临时文件
	private void clearTempFiles() {
		File tempFileDir = new File(margs.getTempFileDir());
		if(tempFileDir.exists() && margs.isDeleteTempDir()){
			DataMigratorUtil.deleteDir(tempFileDir);
		}
	}

	//生成需要进行迁移的数据依赖的拆分字段值文件
	private void createTempFiles(){
		for(TableMigrateInfo table:migrateTables){
			//创建具体拆分表中间临时文件
			createTableTempFiles(table);
		}
		executor.shutdown();
		while(true){
			if(executor.isTerminated()){
				break;
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				LOGGER.error("error",e);
			}
		}
	}
	
	private void migrateData() throws SQLException{
		executor =  new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),new ThreadPoolExecutor.CallerRunsPolicy());
		for(TableMigrateInfo table:migrateTables){
			if(!table.isError()){ //忽略已出错的拆分表
				List<DataNodeMigrateInfo> detailList = table.getDataNodesDetail();
				for(DataNodeMigrateInfo info:detailList){
					executor.execute(new DataMigrateRunner(table, info.getSrc(), info.getTarget(), table.getTableName(), info.getTempFile()));
				}
			}
		}
		executor.shutdown();
		while(true){
			if(executor.isTerminated()){
				break;
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				LOGGER.error("error",e);
			}
		}
	}
	
	//缩容需要重新计算表大小
	private void changeSize() throws SQLException {
		for(TableMigrateInfo table:migrateTables){
			if(!table.isExpantion()){
				List<DataNode> oldDn = table.getOldDataNodes();
				long size = 0L;
				for(DataNode dn:oldDn){
					size+=DataMigratorUtil.querySize(dn, table.getTableName());
				}
				table.setSize(size);
			}
		}
	}

	//校验迁移计划中数据迁移情况同数据实际落盘是否一致
	private void validate() throws SQLException {
		for(TableMigrateInfo table:migrateTables){
			if (table.isError()) {
				continue;
			}
			long size = table.getSize().get();
			long factSize = 0L;
			for(DataNode dn:table.getNewDataNodes()){
				factSize+=DataMigratorUtil.querySize(dn, table.getTableName());
			}
			if(factSize != size){
				String message = "migrate error!after migrate should be:"+size+" but fact is:"+factSize;
				table.setError(true);
				table.setErrMessage(message);
			}
		}
		
		//打印最终迁移结果信息
		String title = "migrate result";
		Map<String,String> result = new HashMap<String, String>();
		for(TableMigrateInfo table:migrateTables){
			String resultMessage = table.isError()?"fail! reason: "+table.getErrMessage():"success";
			result.put(table.getSchemaAndTableName(), resultMessage);
		}
		String info = DataMigratorUtil.printMigrateInfo(title, result, "->");
		System.out.println(info);
	}
	
	//清除中间临时文件、导出的迁移数据文件、已被迁移的原始节点冗余数据
	private void clear(){
		for(TableMigrateInfo table:migrateTables){
			makeClearDataGroup(table);
		}
		for(DataNodeClearGroup group:clearGroup){
			clearData(group.getTempFiles(), group.getTableInfo());
		}
	}
	
	//同一主机上的mysql执行按where条件删除数据并发多了性能反而下降很快
	//按照主机ip进行分组，每个主机ip分配一个线程池，线程池大小可配置，默认为当前主机环境cpu核数的一半
	private void makeClearDataGroup(TableMigrateInfo table){
		List<DataNodeMigrateInfo>  list = table.getDataNodesDetail();
		 //将数据节点按主机ip分组，每组分配一个线程池
		for(DataNodeMigrateInfo dnInfo:list){
			DataNode src = dnInfo.getSrc();
			String ip  =src.getIp();
			File f = dnInfo.getTempFile();
			DataNodeClearGroup group = getDataNodeClearGroup(ip,table);
			if(group == null){
				group = new DataNodeClearGroup(ip, table);
				clearGroup.add(group);
			}
			group.getTempFiles().put(f, src);
		}
	}
	
	private DataNodeClearGroup getDataNodeClearGroup(String ip, TableMigrateInfo table){
		DataNodeClearGroup result = null;
		for(DataNodeClearGroup group:clearGroup){
			if(group.getIp().equals(ip) && group.getTableInfo().equals(table)){
				result = group;
			}
		}
		return result;
	}
	
	private void clearData(Map<File,DataNode> map,TableMigrateInfo table){
		if(table.isError()) {
			return;
		}
		ExecutorService executor  =  new ThreadPoolExecutor(margs.getThreadCount(), margs.getThreadCount(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),new ThreadPoolExecutor.CallerRunsPolicy());
		Iterator<Entry<File,DataNode>>  it = map.entrySet().iterator();
		while(it.hasNext()){
			Entry<File,DataNode> et = it.next();
			File f =et.getKey();
			DataNode srcDn  =  et.getValue();
			executor.execute(new DataClearRunner(table, srcDn, f));
		}
		executor.shutdown();
		while(true){
			if(executor.isTerminated()){
				break;
			}
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				LOGGER.error("error",e);
			}
		}
	}

	private void createTempParentDir(String dir){
		File outputDir = new File(dir);
		if(outputDir.exists()){
			DataMigratorUtil.deleteDir(outputDir);
		}
		outputDir.mkdirs();
		outputDir.setWritable(true);
	}
	
	private void createTableTempFiles(TableMigrateInfo table) {
		List<DataNode> oldDn = table.getOldDataNodes();
		//生成迁移中间文件，并生成迁移执行计划
		for(DataNode dn:oldDn){
			executor.execute(new MigratorConditonFilesMaker(table,dn,margs.getTempFileDir(),margs.getQueryPageSize()));
		}
	}
}
