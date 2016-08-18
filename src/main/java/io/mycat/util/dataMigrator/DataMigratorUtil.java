package io.mycat.util.dataMigrator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.util.JdbcUtils;

public class DataMigratorUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DataMigratorUtil.class);
	
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			LOGGER.error("",e);
		}
	}
	
	/**
	 * 添加数据到文件末尾
	 * @param file
	 * @param content
	 * @throws IOException
	 */
	public static void appendDataToFile(File file, String content) throws IOException {   
		RandomAccessFile randomFile = null;  
		try {     
			// 打开一个随机访问文件流，按读写方式     
			randomFile = new RandomAccessFile(file, "rw");     
			// 文件长度，字节数     
			long fileLength = randomFile.length();     
			// 将写文件指针移到文件尾。     
			randomFile.seek(fileLength);     
			randomFile.writeBytes(content);
			content = null;
		} catch (IOException e) {     
			LOGGER.error("appendDataToFile is error!",e);
		} finally{  
			if(randomFile != null){  
				try {  
					randomFile.close();  
				} catch (IOException e) {
					LOGGER.error("error",e);
				}  
			}  
		}  
	}
	
	public static String readDataFromFile(File file,long offset,int length) throws IOException{
		RandomAccessFile randomFile = null;  
		try {     
			// 打开一个随机访问文件流，按读写方式     
			randomFile = new RandomAccessFile(file, "rw");     
			randomFile.seek(offset);
			byte[] buffer = new byte[length];
			randomFile.read(buffer);
			return new String(buffer).trim();
		} catch (IOException e) {     
			throw e;   
		} finally{  
			if(randomFile != null){  
				try {  
					randomFile.close();  
				} catch (IOException e) {
					LOGGER.error("error",e);
				}  
			}  
		}  
	}
	
	/**
	 * 读取逗号分隔的文件数据
	 * @param file 
	 * @param start 文件起始位置
	 * @param length 读取字节数
	 * @return
	 * @throws IOException
	 */
	public static  String readData(File file,long start,int length) throws IOException{
		String data = readDataFromFile(file, start, length);
		if((start+length)<=file.length()){
			data = data.substring(0, data.lastIndexOf(","));
		}

		return data;
	}
	
	public static final int BUFSIZE = 1024 * 8; 
	
	public static void mergeFiles(File outFile, File f) throws IOException {  
        FileChannel outChannel = null;
        FileOutputStream fos = null;
        FileInputStream fis = null;
        try {  
        	fos = new FileOutputStream(outFile,true);
        	fis = new FileInputStream(f);
            outChannel = fos.getChannel();  
            FileChannel fc = fis.getChannel();   
            ByteBuffer bb = ByteBuffer.allocate(BUFSIZE);  
            while(fc.read(bb) != -1){  
            	bb.flip();  
            	outChannel.write(bb);  
            	bb.clear();  
            }  
            fc.close();  
        } catch (IOException e) {  
        	throw e;
        } finally {  
            try {
            	if(fos != null){
            		fos.close();
            	}
            	if(fis != null){
            		fis.close();
            	}
            	if (outChannel != null){
            		outChannel.close();
            	}
            } 
            catch (IOException e) {
				LOGGER.error("error",e);
            }  
        }  
    }
	
	/**
	 * 统计文件有多少行
	 * @param file
	 * @return
	 */
	public static long countLine(File file) throws IOException{
		long count = 0L;
		RandomAccessFile randomFile = null;  
		
		// 打开一个随机访问文件流，按读写方式     
		try {
			randomFile = new RandomAccessFile(file, "rw");
			String s ="";
			while((s=randomFile.readLine())!=null && !s.trim().isEmpty()){
				count++;
			}
		} catch (FileNotFoundException e) {
			throw e;
		}finally{
			if(randomFile != null){  
				try {  
					randomFile.close();  
				} catch (IOException e) {
					LOGGER.error("error",e);
				}  
			}  
		}
		
		return count;
	}
	
	/**
     * 递归删除目录下的所有文件及子目录下所有文件
     * @param dir 将要删除的文件目录
     * @return boolean Returns "true" if all deletions were successful.
     *                 If a deletion fails, the method stops attempting to
     *                 delete and returns "false".
     */
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
    
    //将命令行中的？替换为具体参数
    public static  String paramsAssignment(String cmd,String mark,Object... params){
		List<Object> paramList= Arrays.asList(params);
		for(Object param:paramList){
			cmd = cmd.replaceFirst("\\"+mark, Matcher.quoteReplacement(param.toString()));
		}
		return cmd;
	}
    
    public static Connection getMysqlConnection(DataNode dn) throws SQLException{
    	Connection con = null;
		con = DriverManager.getConnection(dn.getUrl(), dn.getUserName(), dn.getPwd());
    	return con;
    }
    
    public static List<Map<String, Object>> executeQuery(Connection conn, String sql,Object... parameters) throws SQLException{
    	return JdbcUtils.executeQuery(conn, sql, Arrays.asList(parameters));
    }
    
    //查询表数据量
  	public static long querySize(DataNode dn,String tableName) throws SQLException{
  		List<Map<String, Object>> list=null;
  		long size = 0L;
  		Connection con = null;
  		try {
  			con =  getMysqlConnection(dn);
  			list = executeQuery(con, "select count(1) size from "+tableName);
  			size = (long) list.get(0).get("size");
  		} catch (SQLException e) {
  			throw e;
  		}finally{
  			JdbcUtils.close(con);
  		}
  		return size;
  	}
  	
  	public static void createTable(DataNode dn,String table) throws SQLException{
  		Connection con = null;
  		try {
  			con =  getMysqlConnection(dn);
  			JdbcUtils.execute(con, table, new ArrayList<>());
  		} catch (SQLException e) {
  			throw e;
  		}finally{
  			JdbcUtils.close(con);
  		}
  	}
  	
  	/**
  	 * 格式化数据迁移信息
  	 *  +---------title-------+
  	 *  |key1 = value1     |
  	 *  |key2 = value2     |
  	 *  |...                        |
  	 *  +---------------------+
  	 * @param title
  	 * @param map
  	 * @param mark
  	 * @return
  	 */
  	public static  String printMigrateInfo(String title,Map<String,String> map,String mark){
  		StringBuilder result = new StringBuilder(" ");
  		List<String> mergeList = new ArrayList<>();
  		
  		Iterator<Entry<String, String>> itor = map.entrySet().iterator();
  		
  		int maxKeyLength = 0;
  		int maxValueLength = 0;
  		while(itor.hasNext()){
  			Entry<String, String> entry = itor.next();
  			String key = entry.getKey();
  			String value = entry.getValue();
  			maxKeyLength = (key.length()>maxKeyLength)?key.length():maxKeyLength;
  			maxValueLength =  (value.length()>maxValueLength)?value.length():maxValueLength;
  		}
  		
  		int maxLength=maxKeyLength+maxValueLength+2+mark.length();
  		if(maxLength<= title.length()){
  			maxLength = title.length()+8;
  		}
  		itor = map.entrySet().iterator();
  		//合并key和value，并找出长度最大的字符串
  		while(itor.hasNext()){
  			Entry<String, String> entry = itor.next();
  			String key = entry.getKey();
  			String value = entry.getValue();
  			int keyLength = maxKeyLength-key.length();
  			StringBuilder keySb = new StringBuilder(key);
  			for(int i=0;i<keyLength;i++){
  				keySb.append(" ");
  			}
  			key = keySb.toString();
  			
  			String merge = key+" "+mark+" "+value;
  			mergeList.add(merge);
  		}
  		int maxLineLength = 300;//一行显示最大字符数
  		if(maxLength > maxLineLength){
  			maxLength = maxLineLength;
  		}
  		//拼第一行title
  		StringBuilder titleSb = new StringBuilder("+");
  		int halfLength = (maxLength-title.length())/2;
  		for(int i=0;i<halfLength;i++){
  			titleSb.append("-");
  		}
  		titleSb.append(title);
  		for(int i=0;i<(maxLength-halfLength-title.length());i++){
  			titleSb.append("-");
  		}
  		titleSb.append("+\n");
  		result.append(titleSb);
  		
  		List<String> changeList = new ArrayList<>();
  		//调整内容
  		for(int i=0;i<mergeList.size();i++){
  			String content = mergeList.get(i);
  		    if(content.trim().length()>=maxLength){
  		    	String[] str = content.split(mark);
  		    	String key = str[0];
  		    	String value =str[1];
  		    	String[] values = getValues(value,maxLength-maxKeyLength-1-mark.length());
  		    	for(int j=0;j<values.length;j++){
  		    		String s = "";
  		    		if(j > 0){
  		    			StringBuilder keySb = new StringBuilder();
  		    			for(int k=0;k<key.length()+1;k++){
  		    				keySb.append(" ");
  		    			}
  		    			s = keySb.toString()+values[j];
  		    		}else{
  		    			s = key+mark+values[j];
  		    		}
  		    		
  		    		changeList.add(s);
  		    	}
  		    }else{
  		    	changeList.add(content);
  		    }
  		}
  		
  		//拼接内容
  		for(int i=0;i<changeList.size();i++){
  			StringBuilder contentSb = new StringBuilder(" |");
  			String content = changeList.get(i);
  			contentSb.append(content);
  			int length = maxLength-content.length();
  			for(int j=0;j<length;j++){
  				contentSb.append(" ");
  			}
  			contentSb.append("|\n");
  			result.append(contentSb);
  		}
  		StringBuilder endSb = new StringBuilder(" +");
  		for(int i=0;i<maxLength;i++){
  			endSb.append("-");
  		}
  		endSb.append("+\n");
  		result.append(endSb);
  		return result.toString();
  		
  	}
  	
  	public static  <T> boolean isKeyExistIgnoreCase(Map<String,T> map,String key){
		return map.containsKey(key.toLowerCase()) || map.containsKey(key.toUpperCase());
	}
	
	public static <T> T getValueIgnoreCase(Map<String,T> map,String key){
		T result = map.get(key.toLowerCase());
		return  result==null?map.get(key.toUpperCase()):result;
	}
	
	public static Process exeCmdByOs(String cmd) throws IOException{
		Process process = null;
		
		Runtime runtime = Runtime.getRuntime();
		
		String osName = System.getProperty("os.name");
		
		if(osName.toLowerCase().startsWith("win")){
			process = runtime.exec((new String[]{"cmd","/C",cmd}));
		}else{
			process = runtime.exec((new String[]{"sh","-c",cmd}));
		}
		return process;
	}
  	
  	private static String[] getValues(String value, int maxValueLength) {
  		int length = value.length()/maxValueLength;
  		if(value.length()%maxValueLength>0){
  			length+=1;
  		}
  		String[] result = new String[length];
  		for(int i=0;i<length-1;i++){
  			String str = value.substring(i*maxValueLength,i*maxValueLength+maxValueLength);
  			result[i] = str;
  		}
  		String str = value.substring((length-1)*maxValueLength,value.length());
  		result[length-1] = str;
		return result;
	}
  	
}
