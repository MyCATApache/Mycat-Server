package io.mycat.util.dataMigrator;

import io.mycat.config.model.SystemConfig;
import io.mycat.util.cmd.CmdArgs;

import java.io.File;




/**
 * 数据迁移工具依赖参数
 * @author haonan108
 *
 */
public class DataMigratorArgs {

	/** 并行线程数*/
	public static final String THREAD_COUNT = "threadCount";
	
	/** mysqldump命令所在路径 */
	public static final String MYSQL_BIN = "mysqlBin";
	
	/** 数据迁移生成的中间文件指定存放目录*/
	public static final String TEMP_FILE_DIR = "tempFileDir";
	
	/** 使用主数据源还是当前数据源(如果发生主备切换存在数据源选择问题)*/
	public static final String IS_AWAYS_USE_MASTER = "isAwaysUseMaster";
	
	/**生成中间临时文件一次加载的数据量*/
	public static final String QUERY_PAGE_SIZE = "queryPageSize";
	
	public static final String DEL_THRAD_COUNT = "delThreadCount";
	
	/** mysqldump导出中间文件命令操作系统限制长度 */
	public static final String MYSQL_DUMP_CMD_LENGTH = "cmdLength";
	
	public static final String CHARSET = "charset";
	
	/**完成扩容缩容后清除临时文件 默认为true*/
	public static final String DELETE_TEMP_FILE_DIR = "deleteTempFileDir";
	
	
	
	private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors()*2;
	
	private static final int DEFAULT_DEL_THRAD_COUNT  = Runtime.getRuntime().availableProcessors()/2;
	
	private static final int DEFAULT_CMD_LENGTH = 110*1024;//操作系统命令行限制长度 110KB
	
	private static final int DEFAULT_PAGE_SIZE = 100000;//默认一次读取10w条数据
	
	private static final String DEFAULT_CHARSET = "utf8";
	
	private CmdArgs cmdArgs;
	
	public DataMigratorArgs(String[] args){
		cmdArgs = CmdArgs.getInstance(args);
	}
	
	public String getString(String name){
		return cmdArgs.getString(name);
	}
	
	public String getMysqlBin(){
		String result = getString(MYSQL_BIN);
		if(result ==null) {
			return "";
		}
		if(!result.isEmpty() &&!result.endsWith("/")){
			result +="/";
		}
		return result;
	}
	
	public String getTempFileDir(){
		String path = getString(TEMP_FILE_DIR);
		if(null == path || path.trim().isEmpty()){
			return SystemConfig.getHomePath()+File.separator+"temp";
		}
		return path;
	}
	
	public int getThreadCount(){
		String count =getString(THREAD_COUNT);
		if(null == count||count.isEmpty()|| count.equals("0") ){
			return DEFAULT_THREAD_COUNT;
		}
		return Integer.valueOf(count);
	}
	
	public int getDelThreadCount(){
		String count =getString(DEL_THRAD_COUNT);
		if(null == count||count.isEmpty()|| count.equals("0") ){
			return DEFAULT_DEL_THRAD_COUNT;
		}
		return Integer.valueOf(count);
	}
	
	public boolean isAwaysUseMaster(){
		String result = getString(IS_AWAYS_USE_MASTER);
		if(null == result||result.isEmpty()||result.equals("true")){
			return true;
		}
	    return false;
	}
	
	public int getCmdLength(){
		String result = getString(MYSQL_DUMP_CMD_LENGTH);
		if(null  == result||result.isEmpty()){
			return DEFAULT_CMD_LENGTH;
		}
		if(result.contains("*")){
			String[] arr = result.split("\\*");
			int j = 1;
			for (int i = 0; i < arr.length; i++) {
				j *= Integer.valueOf(arr[i]);
			}
			return j;
		}
		return Integer.valueOf(result);
	}
	
	public int getQueryPageSize(){
		String result = getString(QUERY_PAGE_SIZE);
		if(null == result||result.isEmpty()){
			return DEFAULT_PAGE_SIZE;
		}
		return Integer.valueOf(result);
	}
	
	public String getCharSet(){
		String result = getString(CHARSET);
		if(null == result||result.isEmpty()){
			return DEFAULT_CHARSET;
		}
		return result;
	}
	
	public boolean isDeleteTempDir(){
		String result = getString(DELETE_TEMP_FILE_DIR);
		if(null == result||result.isEmpty()||result.equals("true")){
			return true;
		}
		return false;
	}
}
