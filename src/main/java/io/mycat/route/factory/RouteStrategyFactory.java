package io.mycat.route.factory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteStrategy;
import io.mycat.route.impl.DruidMycatRouteStrategy;

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
	private RouteStrategyFactory() {
	    
	}

	
	public static RouteStrategy getRouteStrategy() {
//		if(!isInit) {
//			synchronized(RouteStrategyFactory.class){
//				if(!isInit){
//					init();
//				}
//			}
//		}
		return defaultStrategy;
	}
	
	public static RouteStrategy getRouteStrategy(String parserType) {
//		if(!isInit) {
//			synchronized(RouteStrategyFactory.class){
//				if(!isInit){
//					init();
//				}
//			}
//		}
		return strategyMap.get(parserType);
	}
}
