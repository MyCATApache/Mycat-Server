package io.mycat.server.interceptor.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.interceptor.SQLInterceptor;

public class CreateDatabaseSqlInterceptor implements SQLInterceptor {
	private static final Logger LOGGER = Logger
			.getLogger(CreateDatabaseSqlInterceptor.class);

	private final class CreateDatabaseSqlRunner implements Runnable {
		private int sqltype = 0;
		private String sqls = "";
		public CreateDatabaseSqlRunner(int sqltype, String sqls) {
			this.sqltype = sqltype;
			this.sqls = sqls;
		}
		public void run() {
			try {
				SystemConfig sysconfig = MycatServer.getInstance().getConfig()
						.getSystem();
				
				String sqlInterceptorType = sysconfig.getCreateDatabaseSqlParser();
				//System.out.println("aaaaaaaaa"+sqlInterceptorType);
				if (sqlInterceptorType.equals("createDatabase")) {
					sqls = sqls.trim().toUpperCase();
					//System.out.println("bbbbbbbbbb"+sqlInterceptorType);
					Pattern p = Pattern.compile("(CREATE\\s+DATABASE)");
					Matcher m = p.matcher(sqls);
					//System.out.println("eeeeeeeeeee"+sqls);
					//System.out.println("ccccccccc"+m.find());
					while (m.find()) {
						try {
							throw new Exception("不支持创建数据库！！");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							LOGGER.error("interceptSQL error:不支持创建数据库！！"+e.getMessage());
							//e.printStackTrace();
						}
					}
				}

			} catch (Exception e) {
				LOGGER.error("interceptSQL error:" + e.getMessage());
			}
		}
	}

	/**
	 * escape mysql create database etc
	 */
	@Override
	public String interceptSQL(String sql, int sqlType) {
		// other interceptors put in here ....
		LOGGER.debug("sql interceptSQL:");
		final int sqltype = sqlType;
		final String sqls = DefaultSqlInterceptor.processEscape(sql);
		MycatServer.getInstance().getBusinessExecutor()
				.execute(new CreateDatabaseSqlRunner(sqltype, sqls));
		return sql;
	}

}
