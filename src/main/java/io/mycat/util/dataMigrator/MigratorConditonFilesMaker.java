package io.mycat.util.dataMigrator;

import io.mycat.util.StringUtil;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.JdbcUtils;

import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.util.CollectionUtil;

/**
 * 对具体某个节点重新路由 生成导出数据所依赖的中间文件
 * @author haonan108
 */
public class MigratorConditonFilesMaker implements Runnable{

	private static final Logger LOGGER = LoggerFactory.getLogger(MigratorConditonFilesMaker.class);
	private DataNode srcDn;
	private List<DataNode> newDnList;
	private String column;
	private String tableName;
	private AbstractPartitionAlgorithm alg;
	private String tempFileDir;
	private TableMigrateInfo tableInfo;
	private int newDnSize;
	private int pageSize;
	
	private Map<DataNode,File> files = new HashMap<>();
	
	Map<DataNode,StringBuilder> map = new HashMap<>();//存放节点发生变化的拆分字段字符串数据 key:dn索引 value 拆分字段值,以逗号分隔
	
	public MigratorConditonFilesMaker(TableMigrateInfo tableInfo,DataNode srcDn,String tempFileDir, int pageSize){
		this.tableInfo = tableInfo;
		this.tempFileDir = tempFileDir;
		this.srcDn = srcDn;
		this.newDnList = tableInfo.getNewDataNodes();
		this.column = tableInfo.getColumn();
		this.tableName = tableInfo.getTableName();
		this.alg = tableInfo.getNewRuleAlgorithm();
		this.newDnSize = newDnList.size();
		this.pageSize = pageSize;
	}
	
	@Override
	public void run() {
		if(tableInfo.isError()) {
			return;
		}
		
		long[] count = new long[newDnSize];
    	int page=0;
    	List<Map<String, Object>> list=null;
		
    	Connection con = null;
		try {
			con = DataMigratorUtil.getMysqlConnection(srcDn);
			//创建空的中间临时文件
			createTempFiles();
			
			//暂时只实现mysql的分页查询
			list = DataMigratorUtil.executeQuery(con, "select " 
			        + column+ " from " + tableName + " limit ?,?", page++ * pageSize,
			        pageSize);
			int total = 0; //该节点表总数据量
			
			while (!CollectionUtil.isEmpty(list)) {
				if(tableInfo.isError()) {
					return;
				}
				flushData(false);
    			for(int i=0,l=list.size();i<l;i++){
    				Map<String, Object> sf=list.get(i);
					Object objFieldVal = sf.get(column);
					String filedVal = objFieldVal.toString();
					if (objFieldVal instanceof  String){
						filedVal = "'"+filedVal+"'";
					}
    				Integer newIndex=alg.calculate(StringUtil.removeBackquote(objFieldVal.toString()));
    				total++;
    				DataNode newDn = newDnList.get(newIndex);
    				if(!srcDn.equals(newDn)){
    					count[newIndex]++;
    					map.get(newDn).append(filedVal+",");
    				}
    			}
    			list = DataMigratorUtil.executeQuery(con, "select "
                        + column + " from " + tableName + " limit ?,?", page++ * pageSize,
                        pageSize);
    		}
			flushData(true);
			statisticalData(total,count);
		} catch (Exception e) {
			//发生错误，终止此拆分表所有节点线程任务，记录错误信息，退出此拆分表迁移任务
			String message = "["+tableInfo.getSchemaName()+":"+tableName+"]  src dataNode: "+srcDn.getUrl()+
					" prepare temp files is failed! this table's migrator will exit! "+e.getMessage();
			tableInfo.setError(true);
			tableInfo.setErrMessage(message);
			System.out.println(message);
			LOGGER.error(message, e);
		}finally{
			JdbcUtils.close(con);
		}
	}
	
	//创建中间临时文件
	private void createTempFiles() throws IOException{
		File parentFile = createDirIfNotExist();
		for(DataNode dn:newDnList){
			if(!srcDn.equals(dn)){
				map.put(dn, new StringBuilder());
				createTempFile(parentFile,dn);
			}
		}
	}
	
	
	//中间临时文件 格式: srcDnName-targetDnName.txt   中间文件存在的话会被清除
	private void createTempFile(File parentFile, DataNode dn) throws IOException {
		File f = new File(parentFile,srcDn.getName()+"(old)"+"-"+dn.getName()+"(new).txt");
		if(f.exists()){
			f.delete();
		}
		f.createNewFile();
		files.put(dn, f);
	}
	
	//统计各节点数据迁移信息,并移除空文件
	private void statisticalData(int total, long[] count){
		tableInfo.getSize().addAndGet(total);
		List<DataNodeMigrateInfo> list = tableInfo.getDataNodesDetail();
		List<Long> sizeList = new ArrayList<>();
		for(int i=0;i<count.length;i++){
			long c = count[i];
			sizeList.add(c);
			DataNode targetDn = newDnList.get(i);
			if(count[i]>0){
				DataNodeMigrateInfo info  =new DataNodeMigrateInfo(tableInfo,srcDn, targetDn, files.get(targetDn), c);
				list.add(info);
			}else{
				File f = files.get(targetDn);
				if(f != null && f.exists()){
					f.delete();
				}
				files.remove(targetDn);
			}
		}
		Map<String, String> map = tableInfo.getDnMigrateSize();
		map.put(srcDn.getName()+"["+total+"]", sizeList.toString());
	}
	
	//将迁移字段值写入中间文件,数据超过1024或者要求强制才写入，避免重复打开关闭写入文件
	private void flushData(boolean isForce) throws IOException {
		for(DataNode dn:newDnList){
			StringBuilder sb = map.get(dn);
			if(sb == null) {
				continue;
			}
			if((isForce || sb.toString().getBytes().length>1024) && sb.length()>0){
				String s = sb.toString();
				if(isForce){//最后一次将末尾的','截掉
					s = s.substring(0, s.length()-1);
				}
				DataMigratorUtil.appendDataToFile(files.get(dn),s);
				sb = new StringBuilder();
				map.put(dn, sb);
			}
		}
	}
	
	//创建中间临时文件父目录
	private File createDirIfNotExist() {
		File f = new File(tempFileDir,tableInfo.getSchemaName()+"-"+tableName);
		if(!f.exists()){
			f.mkdirs();
		}
		return f;
	}
}
