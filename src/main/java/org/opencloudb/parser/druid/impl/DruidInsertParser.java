package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import org.opencloudb.parser.druid.MycatSchemaStatVisitor;
import org.opencloudb.parser.druid.RouteCalculateUnit;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.StringUtil;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

public class DruidInsertParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException {
		
	}
	
	/**
	 * 考虑因素：isChildTable、批量、是否分片
	 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlInsertStatement insert = (MySqlInsertStatement)stmt;
		String tableName = StringUtil.removeBackquote(insert.getTableName().getSimpleName()).toUpperCase();

		ctx.addTable(tableName);
		if(RouterUtil.isNoSharding(schema,tableName)) {//整个schema都不分库或者该表不拆分
			RouterUtil.routeForTableMeta(rrs, schema, tableName, rrs.getStatement());
			rrs.setFinishedRoute(true);
			return;
		}

		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema "
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		} else {
			//childTable的insert直接在解析过程中完成路由
			if (tc.isChildTable()) {
				parserChildTable(schema, rrs, tableName, insert);
				return;
			}
			
			String partitionColumn = tc.getPartitionColumn();
			
			if(partitionColumn != null) {//分片表
				//拆分表必须给出column list,否则无法寻找分片字段的值
				if(insert.getColumns() == null || insert.getColumns().size() == 0) {
					throw new SQLSyntaxErrorException("partition table, insert must provide ColumnList");
				}
				
				//批量insert
				if(isMultiInsert(insert)) {
//					String msg = "multi insert not provided" ;
//					LOGGER.warn(msg);
//					throw new SQLNonTransientException(msg);
					parserBatchInsert(schema, rrs, partitionColumn, tableName, insert);
				} else {
					parserSingleInsert(schema, rrs, partitionColumn, tableName, insert);
				}
				
			}
		}
	}
	
	/**
	 * 寻找joinKey的索引
	 * @param columns
	 * @param joinKey
	 * @return -1表示没找到，>=0表示找到了
	 */
	private int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
		for(int i = 0; i < columns.size(); i++) {
			String col = StringUtil.removeBackquote(columns.get(i).toString()).toUpperCase();
			if(col.equals(joinKey)) {
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
	 * @param insertStmt
	 * @return
	 */
	private boolean isMultiInsert(MySqlInsertStatement insertStmt) {
		return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1) || insertStmt.getQuery() != null;
	}
	
	private RouteResultset parserChildTable(SchemaConfig schema, RouteResultset rrs,
			String tableName, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		TableConfig tc = schema.getTables().get(tableName);
		
		String joinKey = tc.getJoinKey();
		int joinKeyIndex = getJoinKeyIndex(insertStmt.getColumns(), joinKey);
		if(joinKeyIndex == -1) {
			String inf = "joinKey not provided :" + tc.getJoinKey()+ "," + insertStmt;
			LOGGER.warn(inf);
			throw new SQLNonTransientException(inf);
		}
		if(isMultiInsert(insertStmt)) {
			String msg = "ChildTable multi insert not provided" ;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		
		String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();

		
		String sql = insertStmt.toString();
		
		// try to route by ER parent partion key
		RouteResultset theRrs = RouterUtil.routeByERParentKey(null,schema, ServerParse.INSERT,sql, rrs, tc,joinKeyVal);
		if (theRrs != null) {
			rrs.setFinishedRoute(true);
			return theRrs;
		}

		// route by sql query root parent's datanode
		String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + joinKeyVal;
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("find root parent's node sql "+ findRootTBSql);
		}
		FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler();
		String dn = fetchHandler.execute(schema.getName(),findRootTBSql, tc.getRootParent().getDataNodes());
		if (dn == null) {
			throw new SQLNonTransientException("can't find (root) parent sharding node for sql:"+ sql);
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("found partion node for child table to insert "+ dn + " sql :" + sql);
		}
		return RouterUtil.routeToSingleNode(rrs, dn, sql);
	}
	
	/**
	 * 单条insert（非批量）
	 * @param schema
	 * @param rrs
	 * @param partitionColumn
	 * @param tableName
	 * @param insertStmt
	 * @throws SQLNonTransientException
	 */
	private void parserSingleInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn,
			String tableName, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		boolean isFound = false;
		for(int i = 0; i < insertStmt.getColumns().size(); i++) {
			if(partitionColumn.equalsIgnoreCase(StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {//找到分片字段
				isFound = true;
				String column = StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString());
				
				String value = StringUtil.removeBackquote(insertStmt.getValues().getValues().get(i).toString());
				
				RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
				routeCalculateUnit.addShardingExpr(tableName, column, value);
				ctx.addRouteCalculateUnit(routeCalculateUnit);
				//mycat是单分片键，找到了就返回
				break;
			}
		}
		if(!isFound) {//分片表的
			String msg = "bad insert sql (sharding column:"+ partitionColumn + " not provided," + insertStmt;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		// insert into .... on duplicateKey 
		//such as :INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE b=VALUES(b); 
		//INSERT INTO TABLEName (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
		if(insertStmt.getDuplicateKeyUpdate() != null) {
			List<SQLExpr> updateList = insertStmt.getDuplicateKeyUpdate();
			for(SQLExpr expr : updateList) {
				SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr)expr;
				String column = StringUtil.removeBackquote(opExpr.getLeft().toString().toUpperCase());
				if(column.equals(partitionColumn)) {
					String msg = "Sharding column can't be updated: " + tableName + " -> " + partitionColumn;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
			}
		}
	}
	
	/**
	 * insert into .... select .... 或insert into table() values (),(),....
	 * @param schema
	 * @param rrs
	 * @param insertStmt
	 * @throws SQLNonTransientException
	 */
	private void parserBatchInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn, 
			String tableName, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
		//insert into table() values (),(),....
		if(insertStmt.getValuesList().size() > 1) {
			//字段列数
			int columnNum = insertStmt.getColumns().size();
			int shardingColIndex = getSharingColIndex(insertStmt, partitionColumn);
			if(shardingColIndex == -1) {
				String msg = "bad insert sql (sharding column:"+ partitionColumn + " not provided," + insertStmt;
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			} else {
				List<ValuesClause> valueClauseList = insertStmt.getValuesList();
				
				Map<Integer,List<ValuesClause>> nodeValuesMap = new HashMap<Integer,List<ValuesClause>>();
				TableConfig tableConfig = schema.getTables().get(tableName);
				AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
				for(ValuesClause valueClause : valueClauseList) {
					if(valueClause.getValues().size() != columnNum) {
						String msg = "bad insert sql columnSize != valueSize:"
					             + columnNum + " != " + valueClause.getValues().size() 
					             + "values:" + valueClause;
						LOGGER.warn(msg);
						throw new SQLNonTransientException(msg);
					}
					SQLExpr expr = valueClause.getValues().get(shardingColIndex);
					String shardingValue = null;
					if(expr instanceof SQLIntegerExpr) {
						SQLIntegerExpr intExpr = (SQLIntegerExpr)expr;
						shardingValue = intExpr.getNumber() + "";
					} else if (expr instanceof SQLCharExpr) {
						SQLCharExpr charExpr = (SQLCharExpr)expr;
						shardingValue = charExpr.getText();
					}
					
					Integer nodeIndex = algorithm.calculate(shardingValue);
					//没找到插入的分片
					if(nodeIndex == null) {
						String msg = "can't find any valid datanode :" + tableName 
								+ " -> " + partitionColumn + " -> " + shardingValue;
						LOGGER.warn(msg);
						throw new SQLNonTransientException(msg);
					}
					if(nodeValuesMap.get(nodeIndex) == null) {
						nodeValuesMap.put(nodeIndex, new ArrayList<ValuesClause>());
					}
					nodeValuesMap.get(nodeIndex).add(valueClause);
				}
				
				RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
				int count = 0;
				for(Map.Entry<Integer,List<ValuesClause>> node : nodeValuesMap.entrySet()) {
					Integer nodeIndex = node.getKey();
					List<ValuesClause> valuesList = node.getValue();
					insertStmt.setValuesList(valuesList);
					nodes[count++] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex),
							rrs.getSqlType(),insertStmt.toString());
				}
				rrs.setNodes(nodes);
				rrs.setFinishedRoute(true);
			}
		} else if(insertStmt.getQuery() != null) { // insert into .... select ....
			String msg = "TODO:insert into .... select .... not supported!";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
	}
	
	/**
	 * 寻找拆分字段在 columnList中的索引
	 * @param insertStmt
	 * @param partitionColumn
	 * @return
	 */
	private int getSharingColIndex(MySqlInsertStatement insertStmt,String partitionColumn) {
		int shardingColIndex = -1;
		for(int i = 0; i < insertStmt.getColumns().size(); i++) {
			if(partitionColumn.equalsIgnoreCase(StringUtil.removeBackquote(insertStmt.getColumns().get(i).toString()))) {//找到分片字段
				shardingColIndex = i;
				return shardingColIndex;
			}
		}
		return shardingColIndex;
	}
}
