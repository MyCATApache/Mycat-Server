package io.mycat.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 忽略部分SET 指令
 * 
 * 实际使用中PHP用户经常会操作多个SET指令组成一个Stmt , 所以该指令检测功能独立出来
 * 
 * @author zhuam
 *
 */
public class SetIgnoreUtil {
	
	private static List<Pattern> ptrnIgnoreList = new ArrayList<Pattern>();
	
	static  {
		
		//TODO: 忽略部分 SET 指令, 避免WARN 不断的刷日志
		String[] ignores = new String[] {
			"(?i)set (sql_mode)",
			"(?i)set (interactive_timeout|wait_timeout|net_read_timeout|net_write_timeout|lock_wait_timeout|slave_net_timeout)",
			"(?i)set (connect_timeout|delayed_insert_timeout|innodb_lock_wait_timeout|innodb_rollback_on_timeout)",
			"(?i)set (profiling|profiling_history_size)"
		};
		
		for (int i = 0; i < ignores.length; ++i) {
            ptrnIgnoreList.add(Pattern.compile(ignores[i]));
        }
	}
	
	public static boolean isIgnoreStmt(String stmt) {
		boolean ignore = false;
        Matcher matcherIgnore;
        for (Pattern ptrnIgnore : ptrnIgnoreList) {
            matcherIgnore = ptrnIgnore.matcher( stmt );
            if (matcherIgnore.find()) {
                ignore = true;
                break;
            }
        }		
        return ignore;
	}

}
