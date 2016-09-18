package io.mycat.util.dataMigrator;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 数据迁移过程类
 * @author haonan108
 *
 */
public  class DataMigrateRunner implements Runnable{

	private static final Logger LOGGER = LoggerFactory.getLogger(DataMigrateRunner.class);
	private DataNode src;
	private DataNode target;
	private String tableName;
	private DataIO dataIO;
	private File conditionFile;
	private TableMigrateInfo table;
	
	
	
	public DataMigrateRunner(TableMigrateInfo table, DataNode src,DataNode target,String tableName,File conditionFile){
		this.tableName = tableName;
		this.conditionFile= conditionFile;
		this.src = src;
		this.target = target;
		this.table = table;
		dataIO = DataIOFactory.createDataIO(src.getDbType());
	}

	@Override
	public void run() {
		if(table.isError()) {
			return;
		}
		try {
			long start = System.currentTimeMillis();
			File loadFile = dataIO.exportData(table,src, tableName, conditionFile.getParentFile(), conditionFile);
			dataIO.importData(table,target,tableName, loadFile);
			long end = System.currentTimeMillis();
			System.out.println(table.getSchemaAndTableName()+" "+src.getName()+"->"+target.getName()+" completed in "+(end-start)+"ms");
		} catch (Exception e) {
			String errMessage = table.getSchemaAndTableName()+" "+src.getName()+"->"+target.getName()+" migrate err! "+e.getMessage();
			LOGGER.error(errMessage, e);
			table.setError(true);
			table.getErrMessage().append(errMessage);
		}
	}
	
}
