package io.mycat.util.dataMigrator;

import com.alibaba.druid.util.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
		long offset = 0;
		Connection con = null;
		try {
			long start = System.currentTimeMillis();
			con = DataMigratorUtil.getMysqlConnection(srcDn);
			if(tableInfo.isExpantion()){
				deleteDataDependFile(data, offset, con);
			}else{
				//缩容，移除的节点直接truncate删除数据，非移除的节点按照临时文件的中值进行删除操作
				List<DataNode> list = tableInfo.getRemovedDataNodes();
				boolean isRemovedDn = false;
				for(DataNode dn:list){
					if(srcDn.equals(dn)){
						isRemovedDn = true;
					}
				}
				if(isRemovedDn){
					String sql = "truncate "+tableInfo.getTableName();
					JdbcUtils.execute(con, sql, new ArrayList<>());
				}else{
					deleteDataDependFile(data, offset, con);
				}
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
	
	private void deleteDataDependFile(String data,long offset,Connection con) throws IOException, SQLException{
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
	}
}
