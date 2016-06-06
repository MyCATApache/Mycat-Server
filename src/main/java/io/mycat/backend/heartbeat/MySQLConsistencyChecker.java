/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.backend.heartbeat;

import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author digdeep@126.com
 */
public class MySQLConsistencyChecker{
	public static final Logger LOGGER = LoggerFactory.getLogger(MySQLConsistencyChecker.class);
	private final MySQLDataSource source;
	private final ReentrantLock lock;
	private AtomicInteger jobCount = new AtomicInteger();
	private String countSQL;
	private String maxSQL;
	private String tableName;	// global table name
	private long beginTime;
//	private String columnExistSQL = "select count(*) as "+GlobalTableUtil.INNER_COLUMN
//							+ " from information_schema.columns where column_name='"
//							+ GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN + "' and table_name='";
	
	// 此处用到了 mysql 多行转一行 group_concat 的用法，主要是为了简化对结果的处理
	// 得到的结果类似于：id,name,_mycat_op_time
	private String columnExistSQL = "select group_concat(COLUMN_NAME separator ',') as "
			+ GlobalTableUtil.INNER_COLUMN +" from information_schema.columns where TABLE_NAME='"; //user' and TABLE_SCHEMA='db1';
	
	private List<SQLQueryResult<Map<String, String>>> list = new ArrayList<>();

	
	public MySQLConsistencyChecker(MySQLDataSource source, String tableName) {
		this.source = source;
		this.lock = new ReentrantLock(false);
		this.tableName = tableName;
		this.countSQL = " select count(*) as "+GlobalTableUtil.COUNT_COLUMN+" from " 
							+ this.tableName;
		this.maxSQL = " select max("+GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN+") as "+
						GlobalTableUtil.MAX_COLUMN+" from " + this.tableName;
		this.columnExistSQL += this.tableName +"' ";
	}

	public void checkRecordCout() {
        // ["db3","db2","db1"]
		lock.lock();
		try{
			this.jobCount.set(0);
			beginTime = new Date().getTime();
	        String[] physicalSchemas = source.getDbPool().getSchemas();
	        for(String dbName : physicalSchemas){
	        	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null);
	        	OneRawSQLQueryResultHandler resultHandler = 
	        			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.COUNT_COLUMN}, detector);
	        	SQLJob sqlJob = new SQLJob(this.getCountSQL(), dbName, resultHandler, source);
	        	detector.setSqlJob(sqlJob);
	 		    sqlJob.run();
	 		    this.jobCount.incrementAndGet();
	        }
		}finally{
			lock.unlock();
		}
	}
	
	public void checkMaxTimeStamp() {
        // ["db3","db2","db1"]
		lock.lock();
		try{
			this.jobCount.set(0);
			beginTime = new Date().getTime();
	        String[] physicalSchemas = source.getDbPool().getSchemas();
	        for(String dbName : physicalSchemas){
	        	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null);
	        	OneRawSQLQueryResultHandler resultHandler = 
	        			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.MAX_COLUMN}, detector);
	        	SQLJob sqlJob = new SQLJob(this.getMaxSQL(), dbName, resultHandler, source);
	        	detector.setSqlJob(sqlJob);
	 		    sqlJob.run();
	 		    this.jobCount.incrementAndGet();
	        }
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * check inner column exist or not
	 */
	public void checkInnerColumnExist() {
        // ["db3","db2","db1"]
		lock.lock();
		try{
			this.jobCount.set(0);
			beginTime = new Date().getTime();
	        String[] physicalSchemas = source.getDbPool().getSchemas();
	        for(String dbName : physicalSchemas){
	        	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null, 1);
	        	OneRawSQLQueryResultHandler resultHandler = 
	        			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.INNER_COLUMN}, detector);
	        	String db = " and table_schema='" + dbName + "'";
	        	SQLJob sqlJob = new SQLJob(this.columnExistSQL + db , dbName, resultHandler, source);
	        	detector.setSqlJob(sqlJob);//table_schema='db1'
	        	LOGGER.debug(sqlJob.toString());
	 		    sqlJob.run();
	 		    this.jobCount.incrementAndGet();
	        }
		}finally{
			lock.unlock();
		}
	}
	
	public void setResult(SQLQueryResult<Map<String, String>> result) {
		// LOGGER.debug("setResult::::::::::" + JSON.toJSONString(result));
		lock.lock();
		try{
			this.jobCount.decrementAndGet();
			if(result != null && result.isSuccess()){
				result.setTableName(tableName);
				list.add(result);
			}else{
				if(result != null && result.getResult() != null){
					String sql = null;
					if(result.getResult().containsKey(GlobalTableUtil.COUNT_COLUMN))
						sql = this.getCountSQL();
					if(result.getResult().containsKey(GlobalTableUtil.MAX_COLUMN))
						sql = this.getMaxSQL();
					if(result.getResult().containsKey(GlobalTableUtil.INNER_COLUMN))
						sql = this.getColumnExistSQL();
					LOGGER.warn(sql+ " execute failed in db: " + result.getDataNode()
								 + " during global table consistency check task.");
				}
			}
			if(this.jobCount.get() <= 0 || isTimeOut()){
				GlobalTableUtil.finished(list);
	    	}
		}finally{
			lock.unlock();
		}
	}
	
	public boolean isTimeOut(){
		long duration = new Date().getTime() - this.beginTime;
		return TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS) > 1; // 1分钟超时
	}
	
	public String getCountSQL() {
		return countSQL;
	}
	public String getColumnExistSQL() {
		return columnExistSQL;
	}
	public void setColumnExistSQL(String columnExistSQL) {
		this.columnExistSQL = columnExistSQL;
	}
	public String getMaxSQL() {
		return maxSQL;
	}
	public String getTableName() {
		return tableName;
	}
	public MySQLDataSource getSource() {
		return source;
	}
}