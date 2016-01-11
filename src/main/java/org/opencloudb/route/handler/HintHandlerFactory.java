package org.opencloudb.route.handler;

import java.util.HashMap;
import java.util.Map;

public class HintHandlerFactory {
	 //sql注释的类型处理handler 集合，现在支持两种类型的处理：sql,schema
    private static Map<String,HintHandler> hintHandlerMap = null;

    static{
    	hintHandlerMap = new HashMap<String,HintHandler>();
    	init();	// fix原有的多次重复初始化，不断new HintHander的问题；以及是多线程的并发的安全问题; digdeep@126.com
    }
    
    private HintHandlerFactory() {
    }
    
    private static void init() {
        hintHandlerMap.put("sql",new HintSQLHandler());
        hintHandlerMap.put("schema",new HintSchemaHandler());
        hintHandlerMap.put("datanode",new HintDataNodeHandler());
        hintHandlerMap.put("catlet",new HintCatletHandler());
        
        // 新增sql hint（注解）/*#mycat:db_type=master*/ 和 /*#mycat:db_type=slave*/
        // 该hint可以和 /*balance*/ 一起使用
        // 实现强制走 master 和 强制走 slave
        hintHandlerMap.put("db_type", new HintMasterDBHandler());
    }
    
    public static HintHandler getHintHandler(String hintType) {
    	return hintHandlerMap.get(hintType);
    }
    
}
