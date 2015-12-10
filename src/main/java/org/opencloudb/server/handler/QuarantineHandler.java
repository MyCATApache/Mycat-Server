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
package org.opencloudb.server.handler;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.server.ServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;

/**
 * @author songwie
 */
public final class QuarantineHandler {

	private static Logger logger = LoggerFactory.getLogger(QuarantineHandler.class);
	private static boolean check = false;
	
	private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();
	
	public static boolean handle(String sql, ServerConnection c) {
		if(contextLocal.get()==null){
			QuarantineConfig quarantineConfig = MycatServer.getInstance().getConfig().getQuarantine();
            if(quarantineConfig!=null){
            	if(quarantineConfig.isCheck()){
            	   contextLocal.set(quarantineConfig.getProvider());
            	   check = true;
            	}
            }
		}
		if(check){
			WallCheckResult result = contextLocal.get().check(sql);
			if (!result.getViolations().isEmpty()) {
	        	logger.warn(result.getViolations().get(0).getMessage());
	            c.writeErrMessage(ErrorCode.ERR_WRONG_USED, result.getViolations().get(0).getMessage());
	            return false;
	        }
		}
		
		return true;
	}

}