package io.mycat.route.handler;

import io.mycat.route.RouteService;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class HintHandlerFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(HintHandlerFactory.class);
	 //sql注释的类型处理handler 集合
    private static volatile Map<String,HintHandler> hintHandlerMap = null;
    
    static{
    	hintHandlerMap = new HashMap<String,HintHandler>();
    	init();	// 重构fix 线程安全问题和重复初始化重复new HintHandler的问题； by digdeep@126.com
    }
    
    private HintHandlerFactory() {
    }
    
    private static void init() {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
        hintHandlerMap.put("datanode",new HintDataNodeHandler());
        hintHandlerMap.put("catlet",new HintCatletHandler());
        
        // /*#mycat:db_type=master*/, /*#mycat:db_type=slave*/
        // 强制走 master 和 强制走 slave
        hintHandlerMap.put("db_type", new HintMasterDBHandler());
    }
    
    
    public static HintHandler getHintHandler(String hintType) {
    	return hintHandlerMap.get(hintType);
    }
    
}
