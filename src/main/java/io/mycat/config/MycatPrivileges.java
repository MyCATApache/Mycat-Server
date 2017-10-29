/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import io.mycat.config.loader.xml.XMLServerLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.violation.IllegalSQLObjectViolation;

import io.mycat.MycatServer;
import io.mycat.config.model.FirewallConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.config.model.UserPrivilegesConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.net.handler.FrontendPrivileges;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.MycatStatementParser;
import io.mycat.server.ServerConnection;

/**
 * @author mycat
 */
public class MycatPrivileges implements FrontendPrivileges {
	
	private static final String logPath = "/var/log/mycat/";
	
	/**
	 * 无需每次建立连接都new实例。
	 */
	private static MycatPrivileges instance = new MycatPrivileges();
	
    private static final Logger ALARM = LoggerFactory.getLogger("alarm");
    
    private static boolean check = false;	
	private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();

    public static MycatPrivileges instance() {
    	return instance;
    }
    
    private MycatPrivileges() {
    	super();
    }
    
    @Override
    public boolean schemaExists(String schema) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        return conf.getSchemas().containsKey(schema);
    }

    @Override
    public boolean userExists(String user, String host) {
    	//检查用户及白名单
    	return checkFirewallWhiteHostPolicy(user, host);
    }

    @Override
    public String getPassword(String user) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        if (user != null && user.equals(conf.getSystem().getClusterHeartbeatUser())) {
            return conf.getSystem().getClusterHeartbeatPass();
        } else {
            UserConfig uc = conf.getUsers().get(user);
            if (uc != null) {
                return uc.getPassword();
            } else {
                return null;
            }
        }
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
        
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getSchemas();
        } else {
            return null;
        }
    
     }
    
    @Override
    public Boolean isReadOnly(String user) {
        MycatConfig conf = MycatServer.getInstance().getConfig();
       
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.isReadOnly();
        } else {
            return null;
        }
    }

	@Override
	public int getBenchmark(String user) {
		MycatConfig conf = MycatServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getBenchmark();
        } else {
            return 0;
        }
	}

	/**
	 * 防火墙白名单处理，根据防火墙配置，判断目前主机是否可以通过某用户登陆
	 * 白名单配置请参考：
	 * @see  XMLServerLoader
	 * @see  FirewallConfig
	 *
	 * @modification 修改增加网段白名单识别配置
	 * @date 2016/12/8
	 * @modifiedBy Hash Zhang
	 */
	@Override
	public boolean checkFirewallWhiteHostPolicy(String user, String host) {
		
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
        FirewallConfig firewallConfig = mycatConfig.getFirewall();
        
        //防火墙 白名单处理
        boolean isPassed = false;
        
        Map<String, List<UserConfig>> whitehost = firewallConfig.getWhitehost();
        Map<Pattern, List<UserConfig>> whitehostMask = firewallConfig.getWhitehostMask();
        if ((whitehost == null || whitehost.size() == 0)&&(whitehostMask == null || whitehostMask.size() == 0)) {
        	Map<String, UserConfig> users = mycatConfig.getUsers();
        	isPassed = users.containsKey(user);
        	
        } else {
        	List<UserConfig> list = whitehost.get(host);
			Set<Pattern> patterns = whitehostMask.keySet();
			if(patterns != null && patterns.size() > 0){
				for(Pattern pattern : patterns) {
					if(pattern.matcher(host).find()){
						isPassed = true;
						break;
					}
				}
			}
			if (list != null) {
				for (UserConfig userConfig : list) {
					if (userConfig.getName().equals(user)) {
						isPassed = true;
						break;
					}
				}
			}        	
        }
        
        if ( !isPassed ) {
        	 ALARM.error(new StringBuilder().append(Alarms.FIREWALL_ATTACK).append("[host=").append(host)
                     .append(",user=").append(user).append(']').toString());
        	 return false;
        }        
        return true;
	}

	
	/**
	 * @see https://github.com/alibaba/druid/wiki/%E9%85%8D%E7%BD%AE-wallfilter
	 */
	@Override
	public boolean checkFirewallSQLPolicy(FrontendConnection frontendConnection,String user, String sql) {
		
		boolean isPassed = true;
		
		if( contextLocal.get() == null ){
			FirewallConfig firewallConfig = MycatServer.getInstance().getConfig().getFirewall();
			if ( firewallConfig != null) {
				if ( firewallConfig.isCheck() ) {
					contextLocal.set(firewallConfig.getProvider());
					check = true;
				}
			}
		}
		if( check ){
			WallCheckResult result = contextLocal.get().check(sql);
			
			// 修复 druid 防火墙在处理SHOW FULL TABLES WHERE Table_type != 'VIEW' 的时候存在的 BUG
			//List<SQLStatement> stmts =  result.getStatementList();
			//if ( !stmts.isEmpty() &&  !( stmts.get(0) instanceof SQLShowTablesStatement) ) {	//add by hqq 首先判断stmt是否为空有bug：攻击SQL首次执行stmt不为空，再次执行stmt为空，导致拦截失败。			
				if ( !result.getViolations().isEmpty()) {
					
					Violation violation = result.getViolations().get(0);
					//Violation有2个实现类：SyntaxErrorViolation 和 IllegalSQLObjectViolation
					//此处只统计和记录IllegalSQLObjectViolation类型的。
					if(violation instanceof IllegalSQLObjectViolation){
						
						System.out.println("拦截到攻击");
						System.out.println("sql:"+sql);
						System.out.println("user:"+user);
						System.out.println("message:"+violation.getMessage());
						System.out.println("host:"+frontendConnection.getHost());
						System.out.println("schema:"+frontendConnection.getSchema());
						
						//更新累计统计信息
						updateTotalStat(violation.getMessage());
						
						//更新每日统计信息
						updateDayStat(violation.getMessage());
						
						//记录日志详情信息 
						logAudit(frontendConnection, violation.getMessage());
					}
					
					
					isPassed = false;
					ALARM.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:"
							+ result.getViolations().get(0).getMessage() +
							" \r\n " + sql);
		        }				
			//}
			
			
		}
		return isPassed;
	}

	// 审计SQL权限
	@Override
	public boolean checkDmlPrivilege(String user, String schema, String sql) {

		if ( schema == null ) {
			return true;
		}
		
		boolean isPassed = false;

		MycatConfig conf = MycatServer.getInstance().getConfig();
		UserConfig userConfig = conf.getUsers().get(user);
		if (userConfig != null) {
			
			UserPrivilegesConfig userPrivilege = userConfig.getPrivilegesConfig();
			if ( userPrivilege != null && userPrivilege.isCheck() ) {				
			
				UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = userPrivilege.getSchemaPrivilege( schema );
				if ( schemaPrivilege != null ) {
		
					String tableName = null;
					int index = -1;
					
					//TODO 此处待优化，寻找更优SQL 解析器
					
					//修复bug
					// https://github.com/alibaba/druid/issues/1309
					//com.alibaba.druid.sql.parser.ParserException: syntax error, error in :'begin',expect END, actual EOF begin
					if ( sql != null && sql.length() == 5 && sql.equalsIgnoreCase("begin") ) {
						return true;
					}
					
					SQLStatementParser parser = new MycatStatementParser(sql);			
					SQLStatement stmt = parser.parseStatement();
					
					if (stmt instanceof MySqlReplaceStatement || stmt instanceof SQLInsertStatement ) {		
						index = 0;
					} else if (stmt instanceof SQLUpdateStatement ) {
						index = 1;
					} else if (stmt instanceof SQLSelectStatement ) {
						index = 2;
					} else if (stmt instanceof SQLDeleteStatement ) {
						index = 3;
					}
					
					if ( index > -1) {
						
						SchemaStatVisitor schemaStatVisitor = new MycatSchemaStatVisitor();
						stmt.accept(schemaStatVisitor);
						String key = schemaStatVisitor.getCurrentTable();
						if ( key != null ) {
							
							if (key.contains("`")) {
								key = key.replaceAll("`", "");
							}
							
							int dotIndex = key.indexOf(".");
							if (dotIndex > 0) {
								tableName = key.substring(dotIndex + 1);
							} else {
								tableName = key;
							}							
							
							//获取table 权限, 此处不需要检测空值, 无设置则自动继承父级权限
							UserPrivilegesConfig.TablePrivilege tablePrivilege = schemaPrivilege.getTablePrivilege( tableName );
							if ( tablePrivilege.getDml()[index] > 0 ) {
								isPassed = true;
							}
							
						} else {
							//skip
							isPassed = true;
						}
						
						
					} else {						
						//skip
						isPassed = true;
					}
					
				} else {					
					//skip
					isPassed = true;
				}
				
			} else {
				//skip
				isPassed = true;
			}

		} else {
			//skip
			isPassed = true;
		}
		
		if( !isPassed ) {
			 ALARM.error(new StringBuilder().append(Alarms.DML_ATTACK ).append("[sql=").append( sql )
                     .append(",user=").append(user).append(']').toString());
		}
		
		return isPassed;
	}	
	
	/**
	 * 更新累计统计信息 
	 * @param errMsg
	 */
	private static void updateTotalStat(String errMsg){
		if(errMsg == null || errMsg.isEmpty()){
			return;
		}
		File statFile = new File(logPath + "stat.log");
		if(!statFile.exists()){
			try {
				//先创建目录
				if(! statFile.getParentFile().exists()){
					statFile.getParentFile().mkdirs();
				}
				//后创建文件
				statFile.createNewFile();
			} catch (IOException e) {
				ALARM.error("create file stat.log error.", e);
			}
		}
		String fileContent = file2String(statFile);
		Map statMap = null;
		if(fileContent == null || fileContent.isEmpty()){
			statMap = new HashMap();
		}else{
			statMap = (Map)JSONUtils.parse(fileContent);
		}
		if(statMap.get(errMsg) == null){
			statMap.put(errMsg, 1);
		}else{
			statMap.put(errMsg, Integer.parseInt(String.valueOf(statMap.get(errMsg))) + 1);
		}
		String newFileContent = JSONUtils.toJSONString(statMap);
		newFileContent = newFileContent + System.lineSeparator();
		string2File(newFileContent, statFile, false);
	}
	
	/**
	 * 更新每日统计信息
	 * @param errMsg
	 */
	private static void updateDayStat(String errMsg){
		File statDayFile = new File(logPath + "stat_day.log");
		if(!statDayFile.exists()){
			try {
				//先创建目录
				if(! statDayFile.getParentFile().exists()){
					statDayFile.getParentFile().mkdirs();
				}
				//后创建文件
				statDayFile.createNewFile();
			} catch (IOException e) {
				ALARM.error("create file stat_day.log error.", e);
			}
		}
		String statDayFileContent = file2String(statDayFile);
		Map<String,Object> statDayMap = null;
		if(statDayFileContent == null || statDayFileContent.isEmpty()){
			statDayMap = new HashMap();
		}else{
			statDayMap = (Map)JSONUtils.parse(statDayFileContent);
		}
		//新增或更新当前天的统计数据
		String currentDateStr = getDateStr();
		if(statDayMap.get(currentDateStr) == null){
			Map localStatMap = new HashMap();
			localStatMap.put(errMsg, 1);
			statDayMap.put(currentDateStr, localStatMap);
		}else{
			Map localStatMap = (Map)statDayMap.get(currentDateStr);
			if(localStatMap.get(errMsg) == null){
				localStatMap.put(errMsg, 1);
			}else{
				localStatMap.put(errMsg, Integer.parseInt(String.valueOf(localStatMap.get(errMsg))) + 1);
			}
		}
		//删除6个月之前的统计数据
		long currentTimeMillis = getTimeInMillis(currentDateStr,"yyyy-MM-dd");
		List<String> removeKey = new ArrayList<String>(); 
		for(String dateStr : statDayMap.keySet()){  
			//((long)180)*24*3600*1000 的值已超过int最大值，所以需要将第一个乘数转为long
			if(currentTimeMillis - getTimeInMillis(dateStr,"yyyy-MM-dd") > ((long)180)*24*3600*1000){
				removeKey.add(dateStr);
			}
        }  
		for(String key : removeKey){
			statDayMap.remove(key);
		}
		
		//将每日统计信息更新到文件系统
		String newDayFileContent = JSONUtils.toJSONString(statDayMap);
		newDayFileContent = newDayFileContent + System.lineSeparator();
		string2File(newDayFileContent, statDayFile, false);
	}
	
	/**
	 * 记录日志详情
	 * @param c
	 * @param errMsg
	 */
	private static void logAudit(FrontendConnection connection,String errMsg){
		File auditFile = new File(logPath + "audit.log");
		if(!auditFile.exists()){
			try {
				//先创建目录
				if(! auditFile.getParentFile().exists()){
					auditFile.getParentFile().mkdirs();
				}
				//后创建文件
				auditFile.createNewFile();
			} catch (IOException e) {
				ALARM.error("create file audit.log error.", e);
			}
		}
		Map audit = new HashMap();
		audit.put("date", getDateTimeStr());
		audit.put("user", connection.getUser());
		audit.put("host", connection.getHost());
		audit.put("schema", connection.getSchema());
		audit.put("sql", connection.getExecuteSql());
		audit.put("message", errMsg);
		String newAudit = JSONUtils.toJSONString(audit);
		newAudit = newAudit + System.lineSeparator();
		string2File(newAudit, auditFile, true);
	}
	
	public static String file2String(File file) {
		StringBuilder result = new StringBuilder();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String s = null;
			while ((s = br.readLine()) != null) {
				result.append(System.lineSeparator() + s);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
			ALARM.error("read file " + file.getName() + " error.", e);
		}
		return result.toString();
	}
	public static void string2File(String str, File file,boolean isAppend) {
		FileWriter fw = null;
		try {
			fw = new FileWriter(file,isAppend);
			fw.write(str);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	//yyyy-MM-dd HH:mm:ss
	public static String getDateTimeStr() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String res = sdf.format(new Date());
		return res;
	}
	//yyyy-MM-dd 
	public static String getDateStr() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		String res = sdf.format(new Date());
		return res;
	}
	
	public static long getTimeInMillis(String dateStr,String format){
		if(format == null || format.isEmpty()){
			format = "yyyy-MM-dd";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		Date date = null;
		try {
			date = sdf.parse(dateStr);
			return date.getTime();
		} catch (ParseException e) {
			throw new RuntimeException("日期时间格式化错误",e);
		}
	}
	
}