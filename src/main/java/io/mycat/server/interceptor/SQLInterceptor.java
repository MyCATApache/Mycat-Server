package io.mycat.server.interceptor;
/**
 * used for interceptor sql before execute ,can modify sql befor execute
 * 在SQL执行前拦截器，可以在SQL执行前修改
 * @author wuzhih
 *
 */
public interface SQLInterceptor {

	/**
	 * return new sql to handler,ca't modify sql's type 
	 * @param sql
	 * @param sqlType
	 * @return new sql
	 */
	String interceptSQL(String sql ,int sqlType);
}
