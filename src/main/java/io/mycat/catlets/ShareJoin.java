package io.mycat.catlets;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import io.mycat.backend.mysql.nio.handler.MiddlerQueryResultHandler;
import io.mycat.backend.mysql.nio.handler.MiddlerResultHandler;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.AllJobFinishedListener;
import io.mycat.sqlengine.EngineCtx;
import io.mycat.sqlengine.SQLJobHandler;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.sqlengine.mpp.tmp.RowDataSorter;
import io.mycat.util.ByteUtil;
import io.mycat.util.ResultSetUtil;
/**  
 * 功能详细描述:分片join
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2015年01月22日 下午6:50:23 
 * @version 0.0.1
 */

public class ShareJoin implements Catlet {
	private EngineCtx ctx;
	private RouteResultset rrs ;
	private JoinParser joinParser;
	
	private Map<String, byte[]> rows = new ConcurrentHashMap<String, byte[]>();
	private Map<String,String> ids = new ConcurrentHashMap<String,String>();
	//private ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<String>();
	
	private List<byte[]> fields; //主表的字段
	private ArrayList<byte[]> allfields;//所有的字段
	private boolean isMfield=false;
	private int mjob=0;
	private int maxjob=0;
	private int joinindex=0;//关联join表字段的位置
	private int sendField=0;
	private boolean childRoute=false;
	private boolean jointTableIsData=false;
	// join 字段的类型，一般情况都是int, long; 增加该字段为了支持非int,long类型的(一般为varchar)joinkey的sharejoin
 	// 参见：io.mycat.server.packet.FieldPacket 属性： public int type;
 	// 参见：http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
 	private int joinKeyType = Fields.FIELD_TYPE_LONG; // 默认 join 字段为int型
 	
	//重新路由使用
	private SystemConfig sysConfig; 
	private SchemaConfig schema;
	private int sqltype; 
	private String charset; 
	private ServerConnection sc;	
	private LayerCachePool cachePool;
	
	private RowDataSorter sorter; //排序器。
	private volatile boolean isInit = false;
	public void setRoute(RouteResultset rrs){
		this.rrs =rrs;
	}	
	
	public void route(SystemConfig sysConfig, SchemaConfig schema,int sqlType, String realSQL, String charset, ServerConnection sc,	LayerCachePool cachePool) {
		int rs = ServerParse.parse(realSQL);
		this.sqltype = rs & 0xff;
		this.sysConfig=sysConfig; 
		this.schema=schema;
		this.charset=charset; 
		this.sc=sc;	
		this.cachePool=cachePool;		
		try {
		 //  RouteStrategy routes=RouteStrategyFactory.getRouteStrategy();	
		  // rrs =RouteStrategyFactory.getRouteStrategy().route(sysConfig, schema, sqlType2, realSQL,charset, sc, cachePool);		   
			MySqlStatementParser parser = new MySqlStatementParser(realSQL);			
			SQLStatement statement = parser.parseStatement();
			if(statement instanceof SQLSelectStatement) {
			   SQLSelectStatement st=(SQLSelectStatement)statement;
			   SQLSelectQuery sqlSelectQuery =st.getSelect().getQuery();
				if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
					MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)sqlSelectQuery;
					joinParser=new JoinParser(mysqlSelectQuery,realSQL);
					joinParser.parser();
				}	
			}
		   /*	
		   if (routes instanceof DruidMysqlRouteStrategy) {
			   SQLSelectStatement st=((DruidMysqlRouteStrategy) routes).getSQLStatement();
			   SQLSelectQuery sqlSelectQuery =st.getSelect().getQuery();
				if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
					MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)st.getSelect().getQuery();
					joinParser=new JoinParser(mysqlSelectQuery,realSQL);
					joinParser.parser();
				}
		   }
		   */
		} catch (Exception e) {
		
		}
	}
	private void getRoute(String sql){
		try {
		  if (joinParser!=null){
			rrs =RouteStrategyFactory.getRouteStrategy().route(sysConfig, schema, sqltype,sql,charset, sc, cachePool);
		  }
		} catch (Exception e) {
			
		}
	}
	private String[] getDataNodes(){		
		String[] dataNodes =new String[rrs.getNodes().length] ;
		for (int i=0;i<rrs.getNodes().length;i++){
			dataNodes[i]=rrs.getNodes()[i].getName();
		}
		return dataNodes;
	}
	private String getDataNode(String[] dataNodes){
		String dataNode="";
		for (int i=0;i<dataNodes.length;i++){
			dataNode+=dataNodes[i]+",";
		}
		return dataNode;
	}
	
	public void processSQL(String sql, EngineCtx ctx) {
		/*
		 *  获取左边表的sql 获取路由
		 * */
		String ssql=joinParser.getSql();
		getRoute(ssql);
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			ctx.getSession().getSource().writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ ctx.getSession().getSource().getSchema());
			return;
		} 
		this.ctx=ctx;
		//是否可以流式输出
		if(joinParser.hasLimit() || joinParser.hasOrder()){
			ctx.setIsStreamOutputResult(false);
		}
		
		String[] dataNodes =getDataNodes();
		maxjob=dataNodes.length;
	 
		/*
		 *  发送左边表的sql 到每个分片节点去查询
		 * */
    	//huangyiming
		ShareDBJoinHandler joinHandler = new ShareDBJoinHandler(this,joinParser.getJoinLkey(),sc.getSession2());		
		ctx.executeNativeSQLSequnceJob(dataNodes, ssql, joinHandler);
    	EngineCtx.LOGGER.info("Catlet exec:"+getDataNode(getDataNodes())+" sql:" +ssql);
    	final ShareJoin shareJoin = this;
		ctx.setAllJobFinishedListener(new AllJobFinishedListener() {
			@Override
			public void onAllJobFinished(EngineCtx ctx) {				
				 if (!jointTableIsData) {
					 ctx.writeHeader(fields);
				 }
				 
				 MiddlerResultHandler middlerResultHandler = sc.getSession2().getMiddlerResultHandler();

					if(  middlerResultHandler !=null ){
						//sc.getSession2().setCanClose(false);
						middlerResultHandler.secondEexcute(); 
					} else{
						shareJoin.writeEof();
					}
				EngineCtx.LOGGER.info("发送数据OK"); 
			}
		});
	}
	
    

	public void putDBRow(String id,String nid, byte[] rowData,int findex){
    	rows.put(id, rowData);	// 主键 -> 一行数据
    	ids.put(id, nid);       // 主键 ->  joinkey value
    	joinindex=findex;
		//ids.offer(nid);
		int batchSize = 999;
		// 满1000条，发送一个查询请求
		if (ids.size() > batchSize) {
			createQryJob(batchSize);
		}            	
    }
    
    public void putDBFields(List<byte[]> mFields){
    	 if (!isMfield){
    		 fields=mFields; 
    	 }    	
    }    
   
    //发送最后的主表查询语句    
   public void endJobInput(String dataNode, boolean failed){
	   mjob++; //结束任务+1
	   if (mjob>=maxjob){ 
		   //发送最后一个右边表的查询语句。
		 createQryJob(Integer.MAX_VALUE);
	     ctx.endJobInput();
	   }
	  // EngineCtx.LOGGER.info("完成"+mjob+":" + dataNode+" failed:"+failed);
   }
   
	//private void createQryJob(String dataNode,int batchSize) {	
	private void createQryJob(int batchSize) {	
		int count = 0;
		Map<String, byte[]> batchRows = new ConcurrentHashMap<String, byte[]>();
		String theId = null;
		StringBuilder sb = new StringBuilder().append('(');
		String svalue="";
		for(Map.Entry<String,String> e: ids.entrySet() ){
			theId=e.getKey();
			byte[] rowbyte = rows.remove(theId);
			if(rowbyte!=null){
				batchRows.put(theId, rowbyte);
			}			
			if (!svalue.equals(e.getValue())){
				if(joinKeyType == Fields.FIELD_TYPE_VAR_STRING 
						|| joinKeyType == Fields.FIELD_TYPE_STRING){ // joinkey 为varchar
						sb.append("'").append(e.getValue()).append("'").append(','); // ('digdeep','yuanfang') 
				}else{ // 默认joinkey为int/long
					sb.append(e.getValue()).append(','); // (1,2,3) 
				}
			}
			svalue=e.getValue();
			if (count++ > batchSize) {
				break;
			}			
		}
		/*
		while ((theId = ids.poll()) != null) {
			batchRows.put(theId, rows.remove(theId));
			sb.append(theId).append(',');
			if (count++ > batchSize) {
				break;
			}
		}
		*/
		if (count == 0) {
			return;
		}
		jointTableIsData=true;
		sb.deleteCharAt(sb.length() - 1).append(')');
		//select * from tableB where joinKey in (id1,id2);
		String sql = String.format(joinParser.getChildSQL(), sb);
		
		//if (!childRoute){
		  getRoute(sql);
		 //childRoute=true;
		//}
		 
		 //
		ctx.executeNativeSQLParallJob(getDataNodes(),sql, new ShareRowOutPutDataHandler(this,fields,joinindex,joinParser.getJoinRkey(), batchRows,ctx.getSession()));
		EngineCtx.LOGGER.info("SQLParallJob:"+getDataNode(getDataNodes())+" sql:" + sql);		
	}  
	public void writeHeader(String dataNode,List<byte[]> afields, List<byte[]> bfields) {
		sendField++;
		if (sendField==1){		  	
			//huangyiming add 只是中间过程数据不能发送给客户端
			MiddlerResultHandler middlerResultHandler = sc.getSession2().getMiddlerResultHandler();
 			if(middlerResultHandler ==null ){
				 ctx.writeHeader(afields, bfields);
 			}  
 		  setAllFields(afields, bfields);
		 // EngineCtx.LOGGER.info("发送字段2:" + dataNode);		  		   
		}
	   setRowDataSorterHeader(afields, bfields);			    			  		    			

	}
	//设置排序结果收集齐
	private void setRowDataSorterHeader(List<byte[]> afields, List<byte[]> bfields) {
		if(!ctx.getIsStreamOutputResult() && !isInit) { 						
			   synchronized (joinParser) {
				   if(!isInit){
		 			  LinkedHashMap<String, Integer> orderByCols =  joinParser.getOrderByCols();
		 			  LinkedHashMap<String, Integer> childOrderByCols =  joinParser.getChildByCols();
		 			  
		 			  OrderCol[] orderCols = new OrderCol[orderByCols.size() + childOrderByCols.size()];
		 			  //a 表 排序字段
		 			  for(String fileldName : orderByCols.keySet()) {
		 				  ColMeta colMeta =  getCommonFieldIndex(afields, fileldName); //colMeta 放置类型，字段的位置
		 				  int val = orderByCols.get(fileldName);
		 				  int orignIndex =  TableFilter.decodeOrignOrder(val); //字段在排序时候的位子
		 				  int orderType =  TableFilter.decodeOrderType(val); //0:asc or  1:desc
		 				  orderCols[orignIndex] = new OrderCol(colMeta, orderType); 
		 			  }
		 			  
		 			  //b 表
		 			  for(String fileldName : childOrderByCols.keySet()) {
		 				  ColMeta colMeta =  getCommonFieldIndex(bfields, fileldName);
		 				  colMeta.setColIndex(colMeta.getColIndex() + afields.size() -1); // b的字段的位子 在a表字段之后 而且 少了一个joinKey 所以-1 
 		 				  int val = childOrderByCols.get(fileldName);
		 				  int orignIndex =  TableFilter.decodeOrignOrder(val);
		 				  int orderType =  TableFilter.decodeOrderType(val);
		 				  orderCols[orignIndex] = new OrderCol(colMeta, orderType);
		 			  }
		 			  
		 			  RowDataSorter tmp = new RowDataSorter(orderCols);		 			  
		 			  tmp.setLimit(joinParser.getOffset(), joinParser.getRowCount());
		 			  sorter = tmp;
		 			  isInit = true;
				   }
			   }
 		   }
	}
	private void setAllFields(List<byte[]> afields, List<byte[]> bfields){		
		allfields=new ArrayList<byte[]>();
		for (byte[] field : afields) {
			allfields.add(field);
		}
		//EngineCtx.LOGGER.info("所有字段2:" +allfields.size());
		for (int i=1;i<bfields.size();i++){
			allfields.add(bfields.get(i));
		}
		
	}
	public List<byte[]> getAllFields(){		
		return allfields;
	}
	public void writeRow(RowDataPacket rowDataPkg){			
		if(ctx.getIsStreamOutputResult()){
			ctx.writeRow(rowDataPkg);
		} else {			
			if(isInit ){
				//保证可见性
				sorter.addRow(rowDataPkg);	
			} else {				
				EngineCtx.LOGGER.error("===怎么会还没初始化===");
			}
		}
		
	}
	
	protected void writeEof() {		
		boolean t = jointTableIsData;
		if(ctx.getIsStreamOutputResult() || (!jointTableIsData)){
			ctx.writeEof();
		} else {
			//limit 输出。
			int start = joinParser.getOffset();
			int end = start + joinParser.getRowCount();
			List<RowDataPacket> results = sorter.getSortedResult();
			if (start < 0) {
				start = 0;
			}
			
			if (joinParser.getRowCount() <= 0) {
				end = results.size();
			}
			
			if (end > results.size()) {
				end = results.size();
			}			
			for (int i = start; i < end; i++) {
				ctx.writeRow(results.get(i));
			}
			ctx.writeEof();
		}
	}
	//获取fKey在字段 的索引位置。
	public int getFieldIndex(List<byte[]> fields,String fkey){
		int i=0;
		for (byte[] field :fields) {	
			  FieldPacket fieldPacket = new FieldPacket();
			  fieldPacket.read(field);	
			  if (ByteUtil.getString(fieldPacket.orgName).equals(fkey)){
				  joinKeyType = fieldPacket.type;
				  return i;				  
			  }
			  i++;
			}
		return i;		
	}	
	//获取fKey在字段 的索引位置。
	public ColMeta getCommonFieldIndex(List<byte[]> fields,String fkey){
		int i=0;
		for (byte[] field :fields) {	
			  FieldPacket fieldPacket = new FieldPacket();
			  fieldPacket.read(field);	
			  if (ByteUtil.getString(fieldPacket.orgName).equals(fkey)){				  
				  return new ColMeta(i, fieldPacket.type);				  				  
			  }
			  i++;
			}
		return null;		
	}	
}

class ShareDBJoinHandler implements SQLJobHandler {
	private List<byte[]> fields;
	private final ShareJoin ctx;
	private String joinkey;
	private NonBlockingSession session;
	public ShareDBJoinHandler(ShareJoin ctx,String joinField,NonBlockingSession session) {
		super();
		this.ctx = ctx;
		this.joinkey=joinField;
		this.session = session;
		//EngineCtx.LOGGER.info("二次查询:"  +" sql:" + querySQL+"/"+joinkey);
	}

	//private Map<String, byte[]> rows = new ConcurrentHashMap<String, byte[]>();
	//private ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<String>();

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		this.fields = fields;
		ctx.putDBFields(fields);
	}
	

	/*
	public static String getFieldNames(List<byte[]> fields){
		String str="";
		for (byte[] field :fields) {	
		  FieldPacket fieldPacket = new FieldPacket();
		  fieldPacket.read(field);	
		  str+=ByteUtil.getString(fieldPacket.name)+",";
		}
		return str;
	}
	
	public static String getFieldName(byte[] field){
		FieldPacket fieldPacket = new FieldPacket();
		fieldPacket.read(field);	
		return ByteUtil.getString(fieldPacket.name);
	}
	*/
	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		int fid=this.ctx.getFieldIndex(fields,joinkey);
		String id = ResultSetUtil.getColumnValAsString(rowData, fields, 0);//主键，默认id
		String nid = ResultSetUtil.getColumnValAsString(rowData, fields, fid); //joinKey 的value
		// 放入结果集
		//rows.put(id, rowData);
		ctx.putDBRow(id,nid, rowData,fid);
		return false;
	}
	// 收到结束包调用 或者发生错误时候调用。
	@Override
	public void finished(String dataNode, boolean failed, String errorMsg) {
		if(failed){
			session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errorMsg);
		}else{
			ctx.endJobInput(dataNode,failed);
		}
	}

}

class ShareRowOutPutDataHandler implements SQLJobHandler {
	private final List<byte[]> afields; // a表的字段信息
	private List<byte[]> bfields; //B表(右边) 字段信息
	private final ShareJoin ctx; //  sharejoin 的context 
	private final Map<String, byte[]> arows; //	 map a表的记录值  id -> 行记录
	private int joinL;//A表(左边)关联字段的位置
	private int joinR;//B表(右边)关联字段的位置
	private String joinRkey;//B表(右边)关联字段
	public NonBlockingSession session;

	public ShareRowOutPutDataHandler(ShareJoin ctx,List<byte[]> afields,int joini,String joinField,Map<String, byte[]> arows,NonBlockingSession session) {
		super();
		this.afields = afields; // a表的字段信息
		this.ctx = ctx;   //  sharejoin 的context 
		this.arows = arows;	 //	 map a表的记录值  id -> 行记录
		this.joinL =joini; //a 表 joinKey的索引位置。
		this.joinRkey= joinField; // b 表 joinKey的字段名称。
		this.session = session; // mycatSession
		//EngineCtx.LOGGER.info("二次查询:" +arows.size()+ " afields："+FenDBJoinHandler.getFieldNames(afields));
    }

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> bfields) {
		  this.bfields=bfields;		
		  joinR=this.ctx.getFieldIndex(bfields,joinRkey);
		  MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();

			if(  middlerResultHandler ==null ){
				  ctx.writeHeader(dataNode,afields, bfields);

			} 
 	}

	//不是主键，获取join左边的的记录
	private byte[] getRow(Map<String, byte[]> batchRowsCopy,String value,int index){
		for(Map.Entry<String,byte[]> e: batchRowsCopy.entrySet() ){
			String key=e.getKey();
			RowDataPacket rowDataPkg = ResultSetUtil.parseRowData(e.getValue(), afields);
			
			byte[] columnValue = rowDataPkg.fieldValues.get(index); //a 表记录 joinKey的value 
			if(columnValue == null )
				continue;
			
			String id = ByteUtil.getString(columnValue);
			if (id.equals(value)){
				return batchRowsCopy.remove(key);
			}
		}
		return null;
	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		RowDataPacket rowDataPkgold = ResultSetUtil.parseRowData(rowData, bfields);
		//拷贝一份batchRows
		Map<String, byte[]> batchRowsCopy = new ConcurrentHashMap<String, byte[]>();
		batchRowsCopy.putAll(arows);
		// 获取Id字段，
		String id = ByteUtil.getString(rowDataPkgold.fieldValues.get(joinR));
		// 查找ID对应的A表的记录
		byte[] arow = getRow(batchRowsCopy,id,joinL);//arows.remove(id);
//		byte[] arow = getRow(id,joinL);//arows.remove(id);
		while (arow!=null) {
			RowDataPacket rowDataPkg = ResultSetUtil.parseRowData(arow,afields );//ctx.getAllFields());
			//将 b记录的值 复制到 a记录中 成为新的记录。
			for (int i=1;i<rowDataPkgold.fieldCount;i++){
				// 设置b.name 字段
				byte[] bname = rowDataPkgold.fieldValues.get(i);
				rowDataPkg.add(bname);
				rowDataPkg.addFieldCount(1);
			}
			//RowData(rowDataPkg);
			// huangyiming add
			MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
			if(null == middlerResultHandler ){
				// 
				ctx.writeRow(rowDataPkg);
				
			}else{
				
				 if(middlerResultHandler instanceof MiddlerQueryResultHandler){
					// if(middlerResultHandler.getDataType().equalsIgnoreCase("string")){
						 byte[] columnData = rowDataPkg.fieldValues.get(0);
						 if(columnData !=null && columnData.length >0){
 							 String rowValue =    new String(columnData);
							 middlerResultHandler.add(rowValue);	
						 }
				   //}
				 }
				
			} 
			
			arow = getRow(batchRowsCopy,id,joinL);
//		   arow = getRow(id,joinL);
		}
		return false;
	}
	

	@Override
	public void finished(String dataNode, boolean failed, String errorMsg) {
		if(failed){
			session.getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errorMsg);
		}
	}
}