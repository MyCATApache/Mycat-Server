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
package org.opencloudb;

import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.config.Alarms;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.net.handler.FrontendPrivileges;

/**
 * @author mycat
 */
public class MycatPrivileges implements FrontendPrivileges {
	/**
	 * 无需每次建立连接都new实例。
	 */
	private static MycatPrivileges instance = new MycatPrivileges();
	
    private static final Logger ALARM = Logger.getLogger("alarm");

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
        MycatConfig conf = MycatServer.getInstance().getConfig();
        if(conf.getQuarantine().canConnect(host,user)==false){
        	 ALARM.error(new StringBuilder().append(Alarms.QUARANTINE_ATTACK).append("[host=").append(host)
                     .append(",user=").append(user).append(']').toString());
        	 return false;
        }
        return true;
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

	@Override
	public String getBenchmarkSmsTel(String user) {
		MycatConfig conf = MycatServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getBenchmarkSmsTel();
        } else {
            return null;
        }
	}
	
	
}