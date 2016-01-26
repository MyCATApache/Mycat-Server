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
package org.opencloudb.route;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Locale;

import org.apache.log4j.Logger;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.route.handler.HintHandler;
import org.opencloudb.route.handler.HintHandlerFactory;
import org.opencloudb.route.handler.HintSQLHandler;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

public class RouteService {
    private static final Logger LOGGER = Logger
            .getLogger(RouteService.class);
	private final CachePool sqlRouteCache;
	private final LayerCachePool tableId2DataNodeCache;	

	private final String OLD_MYCAT_HINT = "/*!mycat:"; 	// 处理自定义分片注解, 注解格式：/*!mycat: type = value */ sql
	private final String NEW_MYCAT_HINT = "/*#mycat:"; 	// 新的注解格式:/* !mycat: type = value */ sql，oldMycatHint的格式不兼容直连mysql
    private final String HINT_SPLIT = "=";

	public RouteService(CacheService cachService) {
		sqlRouteCache = cachService.getCachePool("SQLRouteCache");
		tableId2DataNodeCache = (LayerCachePool) cachService
				.getCachePool("TableID2DataNodeCache");
	}

	public LayerCachePool getTableId2DataNodeCache() {
		return tableId2DataNodeCache;
	}

	public RouteResultset route(SystemConfig sysconf, SchemaConfig schema,
			int sqlType, String stmt, String charset, ServerConnection sc)
			throws SQLNonTransientException {
		RouteResultset rrs = null;
		String cacheKey = null;

		/**
		 *  SELECT 类型的SQL, 检测
		 */
		if (sqlType == ServerParse.SELECT) {
			cacheKey = schema.getName() + stmt;			
			rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
			if (rrs != null) {
				return rrs;
			}
		}
        
		/*!mycat: sql = select name from aa */
        /*!mycat: schema = test */
        boolean isMatchOldHint = stmt.startsWith(OLD_MYCAT_HINT);
        boolean isMatchNewHint = stmt.startsWith(NEW_MYCAT_HINT);
		if (isMatchOldHint || isMatchNewHint ) {
			
			int endPos = stmt.indexOf("*/");
			if (endPos > 0) {				
				// 用!mycat:内部的语句来做路由分析
				int hintLength = isMatchOldHint ? OLD_MYCAT_HINT.length() : NEW_MYCAT_HINT.length();				
				String hint = stmt.substring(hintLength, endPos).trim();	
				
                int firstSplitPos = hint.indexOf(HINT_SPLIT);                
                if(firstSplitPos > 0 ){
                    
                	String hintType = hint.substring(0,firstSplitPos).trim().toLowerCase(Locale.US);
                    String hintSql = hint.substring(firstSplitPos + HINT_SPLIT.length()).trim();
                    if( hintSql.length() == 0 ) {
                    	LOGGER.warn("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                    	throw new SQLSyntaxErrorException("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                    }
                    String realSQL = stmt.substring(endPos + "*/".length()).trim();

                    HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
                    if( hintHandler != null ) {    

                    	if ( hintHandler instanceof  HintSQLHandler) {                    		
                          	/**
                        	 * 修复 注解SQL的 sqlType 与 实际SQL的 sqlType 不一致问题， 如： hint=SELECT，real=INSERT
                        	 * fixed by zhuam
                        	 */
                    		int hintSqlType = ServerParse.parse( hintSql ) & 0xff;     
                    		rrs = hintHandler.route(sysconf, schema, hintSqlType, realSQL, charset, sc, tableId2DataNodeCache, hintSql);
                    		
                    	} else {                    		
                    		rrs = hintHandler.route(sysconf, schema, sqlType, realSQL, charset, sc, tableId2DataNodeCache, hintSql);
                    	}
 
                    }else{
                        LOGGER.warn("TODO , support hint sql type : " + hintType);
                    }
                    
                }else{//fixed by runfriends@126.com
                	LOGGER.warn("comment in sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                	throw new SQLSyntaxErrorException("comment in sql must meet :/*!mcat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                }
			}
		} else {
			stmt = stmt.trim();
			rrs = RouteStrategyFactory.getRouteStrategy().route(sysconf, schema, sqlType, stmt,
					charset, sc, tableId2DataNodeCache);
		}

		if (rrs != null && sqlType == ServerParse.SELECT && rrs.isCacheAble()) {
			sqlRouteCache.putIfAbsent(cacheKey, rrs);
		}
		return rrs;
	}

}