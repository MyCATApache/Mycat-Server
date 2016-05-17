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
	
	private Runtime runtime = Runtime.getRuntime();
	
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
		
		String loadData ="?mysql -h? -P? -u? -p? -D? --local-infile=1 -e \"load data local infile '?' replace into table ? CHARACTER SET '?' FIELDS TERMINATED BY ','  LINES TERMINATED BY '\\r\\n'\"";
		loadData = DataMigratorUtil.paramsAssignment(loadData,mysqlBin,ip,port,user,pwd,db,file.getAbsolutePath(),tableName,charset);
		LOGGER.debug(table.getSchemaAndTableName()+" "+loadData);
		Process process = runtime.exec((new String[]{"sh","-c",loadData}));
		
		//获取错误信息
		InputStreamReader in = new InputStreamReader(process.getErrorStream());
		BufferedReader br = new BufferedReader(in);
		String errMessage = null;  
        while ((errMessage = br.readLine()) != null) {  
            if(!errMessage.startsWith("Warning")){
            	System.out.println(errMessage+" -> "+loadData);
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
		
		String mysqlDump = "?mysqldump -h? -P? -u? -p? ? ?  --no-create-info --default-character-set=? "
				+ "--add-locks=false --tab='?' --fields-terminated-by=',' --lines-terminated-by='\\r\\n' --where='? in(?)'";
		String fileName = condition.getName();
		File exportPath = new File(export,fileName.substring(0, fileName.indexOf(".txt")));
		if(!exportPath.exists()){
			exportPath.mkdirs();
		}
		//拼接mysqldump命令，不拼接where条件：--where=id in(?)
		mysqlDump = DataMigratorUtil.paramsAssignment(mysqlDump,mysqlBin,ip,port,user,pwd,db,tableName,charset,exportPath,table.getColumn());

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
			String mysqlDumpCmd = DataMigratorUtil.paramsAssignment(mysqlDump,data);
			Process process = runtime.exec((new String[]{"sh","-c",mysqlDumpCmd}));
			//获取错误信息
			InputStreamReader in = new InputStreamReader(process.getErrorStream());
			BufferedReader br = new BufferedReader(in);
			String errMessage = null;  
	        while ((errMessage = br.readLine()) != null) {  
	            if(!errMessage.startsWith("Warning")){
	            	LOGGER.error("err data->"+data);
	            	System.out.println(errMessage+" -> "+mysqlDump);
	            }
	        }
			process.waitFor();
			//查找导出的文件
			File[] files = exportPath.listFiles();
			File exportFile = null;
			for(int i=0;i<files.length;i++){
				if(!files[i].getName().equals(mergedFile.getName())){
					exportFile = files[i];
					break;
				}
			}
			if(exportFile == null){
				 errMessage = "can not find dump file -------> "+mysqlDumpCmd;
				 throw new DataMigratorException(errMessage);
			}
			//合并文件
			DataMigratorUtil.mergeFiles(mergedFile, exportFile);
			
		}
		return mergedFile;
	}
}
