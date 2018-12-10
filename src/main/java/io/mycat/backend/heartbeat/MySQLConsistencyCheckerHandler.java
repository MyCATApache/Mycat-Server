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

import java.util.Date;
import java.util.Map;

import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;


/**
 * @author zwy
 */
public class MySQLConsistencyCheckerHandler extends MySQLConsistencyChecker{
	private final ConsistenCollectHandler handler;
	private volatile int sqlSeq = 1;
	private final PhysicalDBNode dbNode;
	public MySQLConsistencyCheckerHandler(PhysicalDBNode dbNode, MySQLDataSource source,
			String tableName ,ConsistenCollectHandler handler) {
		super(source, tableName);
		this.handler = handler;
		this.dbNode = dbNode;
	}	

    //2
	public void checkRecordCout() {
		this.jobCount.set(0);
		beginTime = new Date().getTime();
		String dbName = dbNode.getDatabase();
    	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null);
    	OneRawSQLQueryResultHandler resultHandler = 
    			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.COUNT_COLUMN}, detector);
    	SQLJob sqlJob = new SQLJob(this.getCountSQL(), dbName, resultHandler, source);
    	detector.setSqlJob(sqlJob);
	    this.jobCount.incrementAndGet();
	    sqlJob.run();
        	
	}
	//1
	public void checkMaxTimeStamp() {
		this.jobCount.set(0);
		beginTime = new Date().getTime();
		String dbName = dbNode.getDatabase();
    	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null, 0);
    	OneRawSQLQueryResultHandler resultHandler = 
    			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.MAX_COLUMN}, detector);
    	SQLJob sqlJob = new SQLJob(this.getMaxSQL(), dbName, resultHandler, source);
    	detector.setSqlJob(sqlJob);
	    this.jobCount.incrementAndGet();      
	    sqlJob.run();
	}
	
	/**
	 * check inner column exist or not
	 */
	//0
	public void checkInnerColumnExist() {
		// ["db3","db2","db1"]
		this.jobCount.set(0);
		beginTime = new Date().getTime();
		String dbName = dbNode.getDatabase();
    	MySQLConsistencyHelper detector = new MySQLConsistencyHelper(this, null, 1);
    	OneRawSQLQueryResultHandler resultHandler = 
    			new OneRawSQLQueryResultHandler(new String[] {GlobalTableUtil.INNER_COLUMN}, detector);
    	String db = " and table_schema='" + dbName + "'";
    	SQLJob sqlJob = new SQLJob(this.columnExistSQL + db , dbName, resultHandler, source);
    	detector.setSqlJob(sqlJob);//table_schema='db1'
	    this.jobCount.incrementAndGet();
    	sqlJob.run();
	
	}
	public volatile boolean isStop = false;
//	volatile SQLQueryResult<Map<String, String>> record =  null;
	volatile SQLQueryResult<Map<String, String>> resultMap = null;
	public void setResult(SQLQueryResult<Map<String, String>> result) {
//		 LOGGER.debug("setResult::::::::::" + JSON.toJSONString(result));
		if(isStop){
			return ;
		}
		if(result != null && result.isSuccess()){	
			jobCount.decrementAndGet();
			String dataNode = result.getDataNode();
			result.setTableName(this.getTableName());
			if(resultMap == null) {
				resultMap = result;
			} else {
				//
				SQLQueryResult<Map<String, String>> r = resultMap;
				Map<String, String> metaData = result.getResult();
				for(String key : metaData.keySet()) {
					r.getResult().put(key, metaData.get(key));
				}
				resultMap = r;
			}
			
		}else{
			if(result != null && result.getResult() != null) {
				String sql = null;				
				final int seq = sqlSeq ;
				if(seq == 0){
					sql = this.getColumnExistSQL();
				} else if(seq == 1) {
					sql = this.getMaxSQL();
				} else if(seq == 2) {
					sql = this.getCountSQL();
				} else {
					sql = result.getErrMsg();
				}
				String errMsg = sql+ " execute failed in db: " + result.getDataNode()
				 + " during global table consistency check task.";
				LOGGER.warn(errMsg);
				handler.onError(errMsg);
			}
		}
		//任务都完成之后 进行下一个sql校验
		if(jobCount.get() == 0 ){
			final int seq = ++sqlSeq ;
			if(seq == 1){
				this.checkMaxTimeStamp();
			} else if(seq == 2) {
				this.checkRecordCout();
			} else {
				handler.onSuccess(resultMap);
				isStop = true;
			}									
		} else if(isTimeOut()){
			String execSql = "";
			final int seq = sqlSeq ;
			if(seq == 0){
				execSql = this.getColumnExistSQL();
			} else if(seq == 1) {
				execSql = this.getMaxSQL();
			} else if(seq == 2) {
				execSql = this.getCountSQL();
			}
			isStop = true;
			handler.onError(String.format("sql %s time out", execSql));
		}	
		
	}
	
	
	
}