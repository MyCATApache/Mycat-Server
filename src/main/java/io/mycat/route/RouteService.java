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
package io.mycat.route;

import io.mycat.cache.CachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.handler.HintHandler;
import io.mycat.route.handler.HintHandlerFactory;
import io.mycat.route.handler.HintMasterDBHandler;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.parser.ServerParse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Locale;

public class RouteService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RouteService.class);
	
	private final CachePool sqlRouteCache;
	private final LayerCachePool tableId2DataNodeCache;
	
	private final String OLD_MYCAT_HINT = "/*!mycat:"; 	// 处理自定义分片注解, 注解格式：/*!mycat: type = value */ sql
	private final String NEW_MYCAT_HINT = "/*#mycat:"; 	// 新的注解格式:/* !mycat: type = value */ sql，oldMycatHint的格式不兼容直连mysql
	private final String HINT_SPLIT = "=";

	public RouteService(CacheService cachService) {
		sqlRouteCache = cachService.getCachePool("SQLRouteCache");
		tableId2DataNodeCache = (LayerCachePool) cachService.getCachePool("TableID2DataNodeCache");
	}

	public LayerCachePool getTableId2DataNodeCache() {
		return tableId2DataNodeCache;
	}

	public RouteResultset route(SystemConfig sysconf, SchemaConfig schema,
			int sqlType, String stmt, String charset, MySQLFrontConnection sc)
			throws SQLNonTransientException {
		RouteResultset rrs = null;
		String cacheKey = null;
		 
		if (sqlType == ServerParse.SELECT) {
			cacheKey = schema.getName() + stmt;
			rrs = (RouteResultset) sqlRouteCache.get(cacheKey);
			if (rrs != null) {
				return rrs;
			}
		}
		 
        /*!mycat: sql = select name from aa */
        /*!mycat: schema = test */
//        boolean isMatchOldHint = stmt.startsWith(OLD_MYCAT_HINT);
//        boolean isMatchNewHint = stmt.startsWith(NEW_MYCAT_HINT);
//		if (isMatchOldHint || isMatchNewHint) {
		int hintLength = RouteService.isHintSql(stmt);
		if(hintLength != -1){
			int endPos = stmt.indexOf("*/");
			if (endPos > 0) {				
				// 用!mycat:内部的语句来做路由分析
//				int hintLength = isMatchOldHint ? OLD_MYCAT_HINT.length() : NEW_MYCAT_HINT.length();
				String hint = stmt.substring(hintLength, endPos).trim();	
                int firstSplitPos = hint.indexOf(HINT_SPLIT);
                
                if(firstSplitPos > 0 ){
                    String hintType = hint.substring(0,firstSplitPos).trim().toLowerCase(Locale.US);
                    
                    String hintValue = hint.substring(firstSplitPos + HINT_SPLIT.length()).trim();
                    if(hintValue.length()==0){
                    	LOGGER.warn("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                    	throw new SQLSyntaxErrorException("comment int sql must meet :/*!mycat:type=value*/ or /*#mycat:type=value*/: "+stmt);
                    }
                    String realSQL = stmt.substring(endPos + "*/".length()).trim();

                    HintHandler hintHandler = HintHandlerFactory.getHintHandler(hintType);
                    if(hintType != null && hintType.equalsIgnoreCase("db_type"))
                    	hintHandler = new HintMasterDBHandler();
                    
                    if(hintHandler != null){
                        rrs = hintHandler.route(sysconf,schema,sqlType,realSQL,charset,sc,tableId2DataNodeCache,hintValue);
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

		if (rrs!=null && sqlType == ServerParse.SELECT && rrs.isCacheAble()) {
			sqlRouteCache.putIfAbsent(cacheKey, rrs);
		}
		return rrs;
	}

	public static int isHintSql(String sql){
		int j = 0;
		int len = sql.length();
		if(sql.charAt(j++) == '/' && sql.charAt(j++) == '*'){
			char c = sql.charAt(j);
			// 过滤掉 空格 和 * 两种字符。
			// 支持： "/** !mycat: */" 和 "/** #mycat: */" 形式的注解. by: digdeep@126.com
			while(j < len && c != '!' && c != '#' && (c == ' ' || c == '*')){
				c = sql.charAt(++j);
			}
			if(j + 6 >= len)	// prevent the following sql.charAt overflow
				return -1;		// false
			if(sql.charAt(++j) == 'm' && sql.charAt(++j) == 'y' && sql.charAt(++j) == 'c'
				&& sql.charAt(++j) == 'a' && sql.charAt(++j) == 't' && sql.charAt(++j) == ':')
				return j+1;	// true，同时返回注解部分的长度
		}
		return -1;	// false
	}
}