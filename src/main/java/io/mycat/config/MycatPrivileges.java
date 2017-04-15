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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;

import io.mycat.MycatServer;
import io.mycat.config.model.FirewallConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.config.model.UserPrivilegesConfig;
import io.mycat.net.handler.FrontendPrivileges;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.MycatStatementParser;

/**
 * @author mycat
 */
public class MycatPrivileges implements FrontendPrivileges {
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
      	//huangyiming 
        MycatConfig conf = MycatServer.getInstance().getConfig();
        int nopassWordLogin = MycatServer.getInstance().getConfig().getSystem().getNonePasswordLogin();
        if(nopassWordLogin == 1){
	        Map<String, SchemaConfig> schemas = conf.getSchemas();
	   		Set<String> result = new HashSet<String>();
	   		if(schemas !=null && schemas.size() > 0 ){
		   	    result =  schemas.keySet();
		   	    return result;
	   		}
        }
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
        //huangyiming add
        int nopassWordLogin = MycatServer.getInstance().getConfig().getSystem().getNonePasswordLogin();
        if(nopassWordLogin == 1){
        	 return false;
        }
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
        	//huangyiming add 
       	 	int nopassWordLogin = MycatServer.getInstance().getConfig().getSystem().getNonePasswordLogin();
            if(nopassWordLogin == 1){
            	isPassed = true;
            }
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
	public boolean checkFirewallSQLPolicy(String user, String sql) {
		
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
			List<SQLStatement> stmts =  result.getStatementList();
			if ( !stmts.isEmpty() &&  !( stmts.get(0) instanceof SQLShowTablesStatement) ) {				
				if ( !result.getViolations().isEmpty()) {				
					isPassed = false;
					ALARM.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:"
							+ result.getViolations().get(0).getMessage() +
							" \r\n " + sql);
		        }				
			}
			
			
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
	
}