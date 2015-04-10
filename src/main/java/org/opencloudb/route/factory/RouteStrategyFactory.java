package org.opencloudb.route.factory;

import java.util.HashMap;
import java.util.Map;

import org.opencloudb.MycatServer;
import org.opencloudb.route.RouteStrategy;
import org.opencloudb.route.impl.DruidMycatRouteStrategy;

/**
 * 路由策略工厂类
 * @author wang.dw
 *
 */
public class RouteStrategyFactory {
	private static RouteStrategy defaultStrategy = null;
	private static boolean isInit = false;
	private static Map<String,RouteStrategy> strategyMap = new HashMap<String,RouteStrategy>();
	
	private static void init() {
		String defaultSqlParser = MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser();
		defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;
		
		strategyMap.put("druidparser", new DruidMycatRouteStrategy());
		
		defaultStrategy = strategyMap.get(defaultSqlParser);
		if(defaultStrategy == null) {
			defaultStrategy = strategyMap.get("druidparser");
		}
	}
	public static RouteStrategy getRouteStrategy() {
		if(!isInit) {
			init();
			isInit = true;
		}
		return defaultStrategy;
	}
	
	public static RouteStrategy getRouteStrategy(String parserType) {
		if(!isInit) {
			init();
			isInit = true;
		}
		return strategyMap.get(parserType);
	}
}
