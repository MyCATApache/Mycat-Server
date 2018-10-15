package io.mycat.util.rehasher;

import io.mycat.util.StringUtil;
import io.mycat.util.cmd.CmdArgs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class RehashCmdArgs {
	public static final String JDBC_DRIVER="jdbcDriver";
	public static final String JDBC_URL="jdbcUrl";
	public static final String HOST="host";
	public static final String USER="user";
	public static final String DATABASE="database";
	public static final String PASSWORD="password";
	public static final String TABLES_FILE="tablesFile";
	public static final String SHARDING_FIELD="shardingField";
	public static final String REHASH_HOSTS_FILE="rehashHostsFile";
	public static final String HASH_TYPE="hashType";
	public static final String SEED="seed";
	public static final String VIRTUAL_BUCKET_TIMES="virtualBucketTimes";
	public static final String WEIGHT_MAP_FILE="weightMapFile";
	public static final String REHASH_NODE_DIR="rehashNodeDir";
	
	
	private CmdArgs cmdArgs;
	
	public RehashCmdArgs(String[] args){
		cmdArgs=CmdArgs.getInstance(args);
	}
	
	public String getString(String name){
		return cmdArgs.getString(name);
	}
	
	public String getJdbcDriver(){
		return getString(JDBC_DRIVER);
	}
	public String getJdbcUrl(){
		return getString(JDBC_URL);
	}
	/**
	 * including host and port, which format is host:port
	 * @return
	 */
	public String getHost(){
		return getString(HOST);
	}
	public String getHostName(){
		String host=getHost();
		return host.substring(0,host.indexOf(':'));
	}
	public int getHostPort(){
		String host=getHost();
		return Integer.parseInt(host.substring(host.indexOf(':')+1));
	}
	public String getDatabase(){
		return getString(DATABASE);
	}
	public String getHostWithDatabase(){
		return getHost()+'/'+getDatabase();
	}
	public String getUser(){
		return getString(USER);
	}
	public String getPassword(){
		return getString(PASSWORD);
	}
	
	public String getTablesFile(){
		return getString(TABLES_FILE);
	}
	public String[] getTables() throws IOException{
		return readStringArrayFromFile(getTablesFile());
	}
	
	public String getShardingField(){
		return getString(SHARDING_FIELD);
	}
	
	public String getRehashHostsFile(){
		return getString(REHASH_HOSTS_FILE);
	}
	public String[] getRehashHosts() throws IOException{
		return readStringArrayFromFile(getRehashHostsFile());
	}
	
	public HashType getHashType(){
		return HashType.valueOf(getString(HASH_TYPE).toUpperCase());
	}

	public int getMurmurHashSeed(){
		return getIntWithDefaultValue(SEED, 0);
	}
	public int getMurmurHashVirtualBucketTimes(){
		return getIntWithDefaultValue(VIRTUAL_BUCKET_TIMES, 160);
	}
	public String getMurmurWeightMapFile(){
		return getString(WEIGHT_MAP_FILE);
	}
	
	public String getRehashNodeDir(){
		return getString(REHASH_NODE_DIR);
	}
	
	private int getIntWithDefaultValue(String name,int defaultValue){
		String val=getString(name);
		if(StringUtil.isEmpty(val)){
			return defaultValue;
		}else{
			return Integer.parseInt(val);
		}
	}
	
	private String[] readStringArrayFromFile(String file) throws IOException{
		BufferedReader br=null;
		try{
			List<String> tableList=new ArrayList<>();
			br=new BufferedReader(new InputStreamReader(new FileInputStream(file),"utf8"));
			String table=null;
			while((table=br.readLine())!=null){
				tableList.add(table);
			}
			return tableList.toArray(new String[tableList.size()]);
		}finally{
			if(br!=null){
				br.close();
			}
		}
	}
}
