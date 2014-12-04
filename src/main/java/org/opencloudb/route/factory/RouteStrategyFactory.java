package org.opencloudb.route.factory;

import org.opencloudb.MycatServer;
import org.opencloudb.route.RouteStrategy;
import org.opencloudb.route.impl.DruidMysqlRouteStrategy;
import org.opencloudb.route.impl.FdbRouteStrategy;

/**
 * 路由策略工厂类
 * @author wang.dw
 *
 */
public class RouteStrategyFactory {
	private static RouteStrategy defaultStrategy = null;
	private static boolean isInit = false;
	private static void init() {
		String defaultSqlParser = MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser();
		defaultSqlParser = defaultSqlParser == null ? "" : defaultSqlParser;
		switch(defaultSqlParser.toLowerCase()) {
		case "fdbparser":
			defaultStrategy = new FdbRouteStrategy();
			break;
		case "druidparser":
			defaultStrategy = new DruidMysqlRouteStrategy();
			break;
		default:
			defaultStrategy = new FdbRouteStrategy();
			break;
		}
	}
	public static RouteStrategy getRouteStrategy() {
		if(!isInit) {
			init();
			isInit = true;
		}
		return defaultStrategy;
	}
}
