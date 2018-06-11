package io.mycat.backend.heartbeat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.math.optimization.GoalType;
import org.apache.logging.log4j.core.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.manager.response.CheckGlobalTable;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.util.ObjectUtil;

public class ConsistenCollectHandler {
    public static final Logger LOGGER = LoggerFactory.getLogger(ConsistenCollectHandler.class);

	private AtomicInteger jobCount = new AtomicInteger(0);
	private AtomicBoolean allJobSend = new AtomicBoolean(false);
	private AtomicBoolean isStop =new AtomicBoolean(false);
	
	private final int retryTime ;
	private final int dnCount ;

	private final  long intervalTime ;
	private final String tableName;
	private final String schemaName;
	private final AtomicInteger successTime;
	private final ManagerConnection con;
	public ConsistenCollectHandler(ManagerConnection c, String tableName, 
			String schemaName, int dnCount, int retryTime, long intervalTime) {
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.retryTime = retryTime;
		this.intervalTime = intervalTime;
		successTime = new AtomicInteger(0);
		this.con = c;
		this.dnCount = dnCount;
	}
	private volatile ScheduledFuture<?> task;
	//定时器不断的检测
	public void startDetector(){
		 task = MycatServer.getInstance().getScheduler()
				.scheduleAtFixedRate(new ConsisterThread(tableName, schemaName, this),
						0, intervalTime, TimeUnit.MILLISECONDS);
	}	
	
	private ReentrantLock lock = new ReentrantLock();
	Map<String, List<SQLQueryResult<Map<String, String>>>> resultMap = new HashMap<>();
	public void onSuccess(SQLQueryResult<Map<String, String>> result) {

		if(isStop.get() == true){
			return ;
		}
		lock.lock();
		if(!resultMap.containsKey(result.getDataNode())) {
			resultMap.put(result.getDataNode(),new ArrayList<SQLQueryResult<Map<String, String>>>());
		}
		resultMap.get(result.getDataNode()).add(result);
		lock.unlock();
		int count = successTime.incrementAndGet();
		LOGGER.info(count + " :{}", JSON.toJSONString(result));
		if (count == retryTime * dnCount) {
			cancelTask();
			String str = "";
			for(List<SQLQueryResult<Map<String, String>>> list : resultMap.values()) {
				str +=  JSONObject.toJSONString(list) + "\n";
			}
			LOGGER.debug(str);
			///数据的校验
			List<SQLQueryResult<Map<String, String>>> unionResult = resultMap.remove(result.getDataNode());
			List<SQLQueryResult<Map<String, String>>> tempResult = null;
			for(List<SQLQueryResult<Map<String, String>>> list : resultMap.values()) {
				tempResult = new ArrayList<>();
				for(SQLQueryResult<Map<String, String>> r1 : list) {
					Map<String, String> md1 = r1.getResult();
					String md1_max_column = md1.get(GlobalTableUtil.MAX_COLUMN);
					for(int i = 0 ; i < unionResult.size(); i++) {
						Map<String, String> md2 = unionResult.get(i).getResult();
						String md2_max_column = md2.get(GlobalTableUtil.MAX_COLUMN);

						if(md1.get(GlobalTableUtil.COUNT_COLUMN).equals(md2.get(GlobalTableUtil.COUNT_COLUMN)) &&
								(md1_max_column==null && null == md2_max_column ) 
								|| ( md1_max_column != null && md1_max_column.equals(md2_max_column))  ) {
							tempResult.add(r1);
							unionResult.remove(unionResult.get(i));
							break;
						}
					}
				}				
				unionResult = tempResult;
				if(unionResult.size() == 0){
					break;
				}				
			}
			
			if(unionResult.size() == 0) {
				LOGGER.debug("check table " +tableName +" not consistence , get info" );
				CheckGlobalTable.response(con, tableName, "No");
			} else {
				LOGGER.debug("check table " +tableName +"  consistence , consistence info" + JSONObject.toJSONString(unionResult));
				System.out.println(JSONObject.toJSONString(unionResult));
				CheckGlobalTable.response(con, tableName, "Yes");

			}			
		}		
	}
	//定时器任务的取消。
	public void cancelTask() {
		final ScheduledFuture<?> t = task;
		if(t != null){
			t.cancel(false);
			task = null;
			
		}
	}
	

	public void onError(String msg) {
		this.cancelTask();
		///将错误消息写回mysql端
		if(isStop.compareAndSet(false, true)) {
			con.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, msg);
		}
	}

	public int getRetryTime() {
		return retryTime;
	}	

}
 class ConsisterThread implements Runnable{
	private final String tableName;
	private final String schemaName;
	private final  ConsistenCollectHandler handler;
	private final AtomicInteger successTime;
	public ConsisterThread(String tableName, String schemaName,ConsistenCollectHandler handler) {
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.handler = handler;
		successTime = new AtomicInteger(0);
	}
	 
	 
	public void run() {
		int count = successTime.incrementAndGet();
		if (count > handler.getRetryTime()) {
			handler.cancelTask();
			ConsistenCollectHandler.LOGGER.info("all check job send finish");
			return;
		}
		try {

			MycatConfig config = MycatServer.getInstance().getConfig();
			TableConfig table = config.getSchemas().get(schemaName).getTables().get(tableName.toUpperCase());

			List<String> dataNodeList = table.getDataNodes();

			// 记录本次已经执行的datanode
			// 多个 datanode 对应到同一个 PhysicalDatasource 只执行一次
			Map<String, String> executedMap = new HashMap<>();
			for (String nodeName : dataNodeList) {
				Map<String, PhysicalDBNode> map = config.getDataNodes();
				for (String k2 : map.keySet()) {
					// <dataNode name="dn1" dataHost="localhost1" database="db1"
					// />
					PhysicalDBNode dBnode = map.get(k2);
					if (nodeName.equals(dBnode.getName())) { // dn1,dn2,dn3
						PhysicalDBPool pool = dBnode.getDbPool(); // dataHost
						Collection<PhysicalDatasource> allDS = pool.genAllDataSources();
						for (PhysicalDatasource pds : allDS) {
							if (pds instanceof MySQLDataSource) {
								MySQLDataSource mds = (MySQLDataSource) pds;
								//if (executedMap.get(pds.getName()) == null) {
								System.out.println(dBnode.getName() + ":" + dBnode.getDatabase());
									MySQLConsistencyChecker checker = new MySQLConsistencyCheckerHandler(dBnode, mds,
											table.getName(), handler);
									checker.checkMaxTimeStamp();
									break;
								//	executedMap.put(pds.getName(), nodeName);
								//}
							}
						}
					}
				}
			}

		} catch (Exception e) {
			ConsistenCollectHandler.LOGGER.error("check consisten err: {}", e);
			handler.onError("ConsisterThread err:" + e.getMessage());
		}
	}
		
	
}
