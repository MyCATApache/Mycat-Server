package io.mycat.util.dataMigrator.dataIOImpl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.util.dataMigrator.DataIO;
import io.mycat.util.dataMigrator.DataMigrator;
import io.mycat.util.dataMigrator.DataMigratorUtil;
import io.mycat.util.dataMigrator.DataNode;
import io.mycat.util.dataMigrator.TableMigrateInfo;
import io.mycat.util.exception.DataMigratorException;

/**
 * mysql导入导出实现类
 * @author haonan108
 *
 */
public class MysqlDataIO implements DataIO{

	private static final Logger LOGGER = LoggerFactory.getLogger(MysqlDataIO.class); 
	
	private String mysqlBin;
	private int cmdLength;
	private String charset;
	
	public MysqlDataIO(){
		cmdLength = DataMigrator.margs.getCmdLength();
		charset = DataMigrator.margs.getCharSet();
		mysqlBin  = DataMigrator.margs.getMysqlBin();
	}
	
	@Override
	public void importData(TableMigrateInfo table,DataNode dn,String tableName, File file) throws IOException, InterruptedException {
		String ip = dn.getIp();
		int port = dn.getPort();
		String user = dn.getUserName();
		String pwd = dn.getPwd();
		String db = dn.getDb();
		
//		String loadData ="?mysql -h? -P? -u? -p? -D? --local-infile=1 -e \"load data local infile '?' replace into table ? CHARACTER SET '?' FIELDS TERMINATED BY ','  LINES TERMINATED BY '\\r\\n'\"";
		String loadData = "?mysql -h? -P? -u? -p? -D?  -f --default-character-set=? -e \"source ?\"";
		loadData = DataMigratorUtil.paramsAssignment(loadData,"?",mysqlBin,ip,port,user,pwd,db,charset,file.getAbsolutePath());
		LOGGER.info(table.getSchemaAndTableName()+" "+loadData);
		Process process = DataMigratorUtil.exeCmdByOs(loadData);
		
		//获取错误信息
		InputStreamReader in = new InputStreamReader(process.getErrorStream());
		BufferedReader br = new BufferedReader(in);
		String errMessage = null;  
        while ((errMessage = br.readLine()) != null) {  
            if(errMessage.trim().toLowerCase().contains("err")){
            	System.out.println(errMessage+" -> "+loadData);
            	throw new DataMigratorException(errMessage+" -> "+loadData);
            }
        }
        
		process.waitFor();
	}

	@Override
	public File exportData(TableMigrateInfo table,DataNode dn, String tableName, File export, File condition) throws IOException, InterruptedException {
		String ip = dn.getIp();
		int port = dn.getPort();
		String user = dn.getUserName();
		String pwd = dn.getPwd();
		String db = dn.getDb();
		
//		String mysqlDump = "?mysqldump -h? -P? -u? -p? ? ?  --no-create-info --default-character-set=? "
//				+ "--add-locks=false --tab='?' --fields-terminated-by=',' --lines-terminated-by='\\r\\n' --where='? in(?)'";
		//由于mysqldump导出csv格式文件只能导出到本地，暂时替换成导出insert形式的文件
		String mysqlDump = "?mysqldump -h? -P? -u? -p? ? ?  --compact --no-create-info --default-character-set=? --add-locks=false --where=\"? in (#)\" --result-file=\"?\"";
		
		String fileName = condition.getName();
		File exportPath = new File(export,fileName.substring(0, fileName.indexOf(".txt")));
		if(!exportPath.exists()){
			exportPath.mkdirs();
		}
		File exportFile = new File(exportPath,tableName.toLowerCase()+".txt");
		//拼接mysqldump命令，不拼接where条件：--where=id in(?)
		mysqlDump = DataMigratorUtil.paramsAssignment(mysqlDump,"?",mysqlBin,ip,port,user,pwd,db,tableName,charset,table.getColumn(),exportFile);

		String data = "";
		//由于操作系统对命令行长度的限制，导出过程被拆分成多次，最后需要将导出的数据文件合并
		File mergedFile = new File(exportPath,tableName.toLowerCase()+".sql");
		if(!mergedFile.exists()){
			mergedFile.createNewFile();
		}
		int offset = 0;
		while((data=DataMigratorUtil.readData(condition,offset,cmdLength)).length()>0){
			offset += data.getBytes().length;
			if(data.startsWith(",")){
				data = data.substring(1, data.length());
			}
			if(data.endsWith(",")){
				data = data.substring(0,data.length()-1);
			}
			String mysqlDumpCmd = DataMigratorUtil.paramsAssignment(mysqlDump,"#",data);
			LOGGER.info(table.getSchemaAndTableName()+mysqlDump);
			LOGGER.debug(table.getSchemaAndTableName()+" "+mysqlDumpCmd);
			
			Process process = DataMigratorUtil.exeCmdByOs(mysqlDumpCmd);  
			//获取错误信息
			InputStreamReader in = new InputStreamReader(process.getErrorStream());
			BufferedReader br = new BufferedReader(in);
			String errMessage = null;  
	        while ((errMessage = br.readLine()) != null) {  
	            if(errMessage.trim().toLowerCase().contains("err")){
	            	System.out.println(errMessage+" -> "+mysqlDump);
	            	throw new DataMigratorException(errMessage+" -> "+mysqlDump);
	            }else{
	            	LOGGER.info(table.getSchemaAndTableName()+mysqlDump+" exe info:"+errMessage);
	            }
	        }
			process.waitFor();

			//合并文件
			DataMigratorUtil.mergeFiles(mergedFile, exportFile);
			if(exportFile.exists()){
				exportFile.delete();
			}
		}
		return mergedFile;
	}
}
