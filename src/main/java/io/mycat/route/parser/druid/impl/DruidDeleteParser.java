package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.Collection;
import java.util.Map;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

import io.mycat.MycatServer;
import io.mycat.cache.CachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.util.StringUtil;

public class DruidDeleteParser extends DefaultDruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlDeleteStatement delete = (MySqlDeleteStatement)stmt;
		String tableName = StringUtil.removeBackquote(delete.getTableName().getSimpleName().toUpperCase());
		ctx.addTable(tableName);

		//在解析SQL时清空该表的主键缓存
//		DefaultLayedCachePool tableID2DataNodeCache=(DefaultLayedCachePool) MycatServer.getInstance().getCacheService()
//				.getCachePool("TableID2DataNodeCache");
//		tableID2DataNodeCache.clearCache(schema.getName().toLowerCase()+"_"+tableName.toUpperCase());
//		tableID2DataNodeCache.getCacheStatic().reset();
		Map<String, CachePool> map = MycatServer.getInstance().getCacheService().getAllCachePools();
		if(map!=null && map.size()>0){
			Collection<CachePool> collection = map.values();
			for(CachePool item : collection){
				if(item instanceof LayerCachePool){
					((LayerCachePool) item).clearCache(schema.getName().toLowerCase()+"_"+tableName.toUpperCase());
					if(item.getCacheStatic()!=null){
						item.getCacheStatic().reset();
					}
				}
			}
		}

	}
}

