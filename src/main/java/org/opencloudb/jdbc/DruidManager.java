package org.opencloudb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.opencloudb.config.model.DBHostConfig;

import com.alibaba.druid.pool.DruidDataSource;

public class DruidManager {
	    private DruidManager() {}
	    private static DruidManager single=null;
	    private DruidDataSource dataSource;
	    private static DBHostConfig cfg = null;

	    public synchronized  static DruidManager getInstance(DBHostConfig config) {
	         if (single == null) {
	             single = new DruidManager();
	             cfg = config;
	             single.initPool();
	         }
	        return single;
	    }
		private void initPool() {
	    	String dbType = cfg.getDbType();
			dataSource = new DruidDataSource();

			if(dbType.equalsIgnoreCase("mysql")){
				dataSource.setDriverClassName("com.mysql.jdbc.Driver");
	        }
			if(dbType.equalsIgnoreCase("mongodb")){
				dataSource.setDriverClassName("org.opencloudb.jdbc.mongodb.MongoDriver");
	        }
			if(dbType.equalsIgnoreCase("oracle")){
				dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
	        }
			if(dbType.equalsIgnoreCase("sqlserver")){
				dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	        }
			if(dbType.equalsIgnoreCase("hive")){
				dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
	        }
			if(dbType.equalsIgnoreCase("db2")){
				dataSource.setDriverClassName("com.ibm.db2.jcc.DB2Driver");
	        }
			if(dbType.equalsIgnoreCase("postgresql")){
				dataSource.setDriverClassName("org.postgresql.Driver");
	        }

			dataSource.setUsername(cfg.getUser());
			dataSource.setPassword(cfg.getPassword());
			dataSource.setUrl(cfg.getUrl());
			dataSource.setInitialSize(cfg.getMinCon());
			dataSource.setMinIdle(1);
			dataSource.setMaxActive(cfg.getMaxCon());
			dataSource.setTimeBetweenLogStatsMillis(cfg.getLogTime());

			// 启用监控统计功能
			 try {
				dataSource.setFilters(cfg.getFilters() );
			} catch (SQLException e) {

			}// for mysql
			 dataSource.setPoolPreparedStatements(false);
		}

		public  Connection getConnection(){
			Connection connection = null;
			try {
				connection = dataSource.getConnection();
			} catch (SQLException e) {

			}
			return connection;
		}
}
