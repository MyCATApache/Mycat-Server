package io.mycat.route;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.ServerConnection;

import java.sql.SQLNonTransientException;

/**
 * 路由策略接口
 * @author wang.dw
 *
 */
public interface RouteStrategy {

    /**
     * 获得路由
     *
     * @param sysConfig 系统配置
     * @param schema schema 配置
     * @param sqlType SQL 类型
     * @param origSQL SQL
     * @param charset charset
     * @param sc 前端服务器连接
     * @param cachePool 缓存
     * @return 路由结果
     * @throws SQLNonTransientException 当数据迁移时
     */
    RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema,int sqlType, String origSQL, String charset, ServerConnection sc, LayerCachePool cachePool)
			throws SQLNonTransientException;
}
