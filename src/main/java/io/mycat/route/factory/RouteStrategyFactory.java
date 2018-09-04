package io.mycat.route.factory;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteStrategy;
import io.mycat.route.impl.DruidMycatRouteStrategy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 路由策略工厂类
 * @author wang.dw
 *
 */
public class RouteStrategyFactory {
	private static RouteStrategy defaultStrategy = null;
	private static volatile boolean isInit = false;
	private static ConcurrentMap<String,RouteStrategy> strategyMap = new ConcurrentHashMap<String,RouteStrategy>();
	public static void init() {
		SystemConfig config = MycatServer.getInstance().getConfig().getSystem();

		String defaultSqlParser = config.getDefaultSqlParser();
		defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;
		//修改为ConcurrentHashMap，避免并发问题
		strategyMap.putIfAbsent("druidparser", new DruidMycatRouteStrategy());

		defaultStrategy = strategyMap.get(defaultSqlParser);
		if(defaultStrategy == null) {
			defaultStrategy = strategyMap.get("druidparser");
			defaultSqlParser = "druidparser";
		}
		config.setDefaultSqlParser(defaultSqlParser);
		isInit = true;
	}

	/**
	 * 获取默认路由策略
	 * @return
	 */
	public static RouteStrategy getRouteStrategy() {
		return defaultStrategy;
	}
	
	public static RouteStrategy getRouteStrategy(String parserType) {
		return strategyMap.get(parserType);
	}

	private RouteStrategyFactory() {}
}
