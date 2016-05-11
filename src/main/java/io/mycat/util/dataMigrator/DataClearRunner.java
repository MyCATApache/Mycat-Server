package io.mycat.util.dataMigrator;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.JdbcUtils;

/**
 * 清理数据扩容缩容后的冗余数据
 * @author haonan108
 *
 */
public class DataClearRunner implements Runnable{

	private static final Logger LOGGER = LoggerFactory.getLogger(DataClearRunner.class);
	private DataNode srcDn;
	private File tempFile;
	private TableMigrateInfo tableInfo;
	
	public DataClearRunner(TableMigrateInfo tableInfo,DataNode srcDn,File tempFile){
		this.tableInfo = tableInfo;
		this.srcDn = srcDn;
		this.tempFile = tempFile;
	}
	@Override
	public void run() {
		String data = "";
		int offset = 0;
		Connection con = null;
		try {
			long start = System.currentTimeMillis();
			con = DataMigratorUtil.getMysqlConnection(srcDn);
			if(tableInfo.isExpantion()){
				while((data=DataMigratorUtil.readData(tempFile,offset,DataMigrator.margs.getQueryPageSize())).length()>0){
					offset += data.getBytes().length;
					if(data.startsWith(",")){
						data = data.substring(1, data.length());
					}
					if(data.endsWith(",")){
						data = data.substring(0,data.length()-1);
					}
					String sql = "delete from "+tableInfo.getTableName()+" where "+tableInfo.getColumn()+" in ("+data+")";
					JdbcUtils.execute(con, sql, new ArrayList<>());
				}
			}else{
				String sql = "truncate "+tableInfo.getTableName();
				JdbcUtils.execute(con, sql, new ArrayList<>());
			}
			long end = System.currentTimeMillis();
			System.out.println(tableInfo.getSchemaAndTableName()+" clean dataNode "+srcDn.getName()+" completed in "+(end-start)+"ms");
			
		} catch (Exception e) {
			String errMessage = srcDn.toString()+":"+"clean data error!";
			LOGGER.error(errMessage, e);
			tableInfo.setError(true);
			tableInfo.getErrMessage().append(errMessage+"\n");
		} finally{
			JdbcUtils.close(con);
		}
	}
}
