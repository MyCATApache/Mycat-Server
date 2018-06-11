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
package io.mycat.globalTableCheck;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;

/**
 * @author zwy
 */
public class MySQLGlobalTableConsistencyChecker{
	public static final Logger LOGGER = LoggerFactory.getLogger(MySQLGlobalTableConsistencyChecker.class);
//	private final MySQLDataSource source;
	private final ReentrantLock lock;
	private AtomicInteger jobCount = new AtomicInteger();
	private String countSQL;
	private String maxSQL;
	private String tableName;	// global table name
	private long beginTime;
	public final static String COLUMN_NAME = "column_name";
	// 得到的结果类似于：id,name,_mycat_op_time
	private String showColumns = "select group_concat(COLUMN_NAME separator ',') as "
			+ COLUMN_NAME +" from information_schema.columns where TABLE_NAME='%s' and TABLE_SCHEMA= '%s' "; //user' and TABLE_SCHEMA='db1';
    private String showPrimarySQL = "show index from %s where Key_name='PRIMARY'";
    private String showUniqueSQL = "show index from %s where Non_unique=0";

	private List<SQLQueryResult<Map<String, String>>> list = new ArrayList<>();

	private final  List<String> dataNodeList ; //全局表对应的DataNode
	public MySQLGlobalTableConsistencyChecker(List<String> dataNodeList, String tableName) {
		this.lock = new ReentrantLock(false);
		this.tableName = tableName;
		this.showUniqueSQL = String.format(this.showUniqueSQL, this.tableName);
		this.showPrimarySQL = String.format(this.showPrimarySQL, this.tableName);
		this.showColumns = String.format(this.showColumns, this.tableName);
		this.dataNodeList = dataNodeList;
	}

	public void getColumnNames() {
        // ["db3","db2","db1"]
		lock.lock();
		try{
			this.jobCount.set(0);
			beginTime = new Date().getTime();
			String dataNodeName = dataNodeList.get(0);			
			PhysicalDBPool pool = MycatServer.getInstance().getConfig().getDataNodes().get(dataNodeName).getDbPool();
			List<PhysicalDatasource> dsList = (List<PhysicalDatasource>)pool.genAllDataSources();
			for(PhysicalDatasource ds : dsList){
				if(ds instanceof MySQLDataSource){
					MySQLDataSource source = (MySQLDataSource)dsList.get(0);
					MySQLGlobalConsistencyHelper detector = new MySQLGlobalConsistencyHelper(this, null);
		        	OneRawSQLQueryResultHandler resultHandler = 
		        			new OneRawSQLQueryResultHandler(new String[] {COLUMN_NAME}, detector);
		        	pool.getSchemas();
		        	SQLJob sqlJob = new SQLJob(this.showColumns, pool.getSchemas()[0], resultHandler, source);
		        	detector.setSqlJob(sqlJob);
		 		    sqlJob.run();
		 		    this.jobCount.incrementAndGet();
				}
			}
		}finally{
			lock.unlock();
		}
	}
	
	public void setResult(SQLQueryResult<Map<String, String>> result) {
		
	}
	
	
	
//	public void setResult(SQLQueryResult<Map<String, String>> result) {
//		// LOGGER.debug("setResult::::::::::" + JSON.toJSONString(result));
//		lock.lock();
//		try{
//			this.jobCount.decrementAndGet();
//			if(result != null && result.isSuccess()){
//				result.setTableName(tableName);
//				list.add(result);
//			}else{
//				if(result != null && result.getResult() != null){
//					String sql = null;
//					if(result.getResult().containsKey(GlobalTableUtil.COUNT_COLUMN))
//						sql = this.getCountSQL();
//					if(result.getResult().containsKey(GlobalTableUtil.MAX_COLUMN))
//						sql = this.getMaxSQL();
//					if(result.getResult().containsKey(GlobalTableUtil.INNER_COLUMN))
//						sql = this.getColumnExistSQL();
//					LOGGER.warn(sql+ " execute failed in db: " + result.getDataNode()
//								 + " during global table consistency check task.");
//				}
//			}
//			if(this.jobCount.get() <= 0 || isTimeOut()){
//				GlobalTableUtil.finished(list);
//	    	}
//		}finally{
//			lock.unlock();
//		}
//	}
//	
//	public boolean isTimeOut(){
//		long duration = new Date().getTime() - this.beginTime;
//		return TimeUnit.MINUTES.convert(duration, TimeUnit.MILLISECONDS) > 1; // 1分钟超时
//	}
//	
//	public String getCountSQL() {
//		return countSQL;
//	}
//	public String getColumnExistSQL() {
//		return showColumns;
//	}
//	public void setColumnExistSQL(String columnExistSQL) {
//		this.showColumns = columnExistSQL;
//	}
//	public String getMaxSQL() {
//		return maxSQL;
//	}
//	public String getTableName() {
//		return tableName;
//	}
//	public MySQLDataSource getSource() {
//		return source;
//	}
}