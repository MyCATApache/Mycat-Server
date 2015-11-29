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
package org.opencloudb.config.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;

import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.WallContext;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;

/**
 * 隔离区配置定义
 * 
 * @author mycat
 */
public final class QuarantineConfig {

    private Map<String, List<UserConfig>> whitehost;
    private List<String> blacklist;
    
    private static WallProvider provider ;
    //private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();
    
    public QuarantineConfig() { }
    
    /*public void init(){
    	WallContext wallContext = WallContext.createIfNotExists("mysql");
    	if(provider==null){
    		provider = new MySqlWallProvider();
    	}
    	List<Violation> violations = new ArrayList<>();
    	provider.setBlackListEnable(true);
    	for(String sql : blacklist){
        	provider.addBlackSql(sql, wallContext.getTableStats(), wallContext.getFunctionStats(), violations, true);
    	}
    }*/
    
    public WallProvider getWallProvider(){
    	return provider;
    }

	public Map<String, List<UserConfig>> getWhitehost() {
		return this.whitehost;
	}
	public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
		this.whitehost = whitehost;
	}
	public List<String> getBlacklist() {
		return this.blacklist;
	}
	public void setBlacklist(List<String> blacklist) {
		this.blacklist = blacklist;
	}
	public boolean existsHost(String host) {
		return this.whitehost==null ? false : whitehost.get(host)!=null ;
	}
	public boolean canConnect(String host,String user) {
		if(whitehost==null){
			MycatConfig config = MycatServer.getInstance().getConfig();
			Map<String, UserConfig> users = config.getUsers();
			return users.containsKey(user);
		}else{
			List<UserConfig> list = whitehost.get(host);
			if(list==null){
				return false;
			}
			for(UserConfig userConfig : list){
				if(userConfig.getName().equals(user)){
					return true;
				}
			}
		}
		return false ;
	}
}