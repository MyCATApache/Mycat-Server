package org.opencloudb.route.handler;

import java.util.HashMap;
import java.util.Map;

public class HintHandlerFactory {
	
	private static volatile boolean isInit = false;
	
	 //sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema
    private static Map<String,HintHandler> hintHandlerMap = new HashMap<String,HintHandler>();

    private HintHandlerFactory() {
    }
    
    private static void init() {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
        hintHandlerMap.put("datanode",new HintDataNodeHandler());
        hintHandlerMap.put("catlet",new HintCatletHandler());
        isInit = true;	// 修复多次初始化的bug
    }
    
    // 双重校验锁 fix 线程安全问题
    public static HintHandler getHintHandler(String hintType) {
    	if(!isInit) {
    		synchronized(HintHandlerFactory.class){
    			if(!isInit)
    				init();
    		}
    	}
    	return hintHandlerMap.get(hintType);
    }
    
}
