package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.function.AbstractPartionAlgorithm;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

public class DruidInsertParser extends DefaultDruidParser {
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		
	}
	
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		MySqlInsertStatement insert = (MySqlInsertStatement)stmt;
		String tableName = removeBackquote(insert.getTableName().getSimpleName()).toUpperCase();
		
		ctx.addTable(tableName);
		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema "
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		} else {
			String partitionColumn = tc.getPartitionColumn();
			
			if(partitionColumn != null) {//分片表
				//拆分表必须给出column list,否则无法寻找分片字段的值
				if(insert.getColumns() == null || insert.getColumns().size() == 0) {
					throw new SQLSyntaxErrorException("partition table, insert must provide ColumnList");
				}
				
				//批量insert
				if(insert.getValuesList().size() > 1 || insert.getQuery() != null) {
					String msg = "multi insert not provided" ;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
//					parserBatchInsert(schema, rrs, partitionColumn, tableName, insert);//TODO 需要修改druid源码支持，先不支持，等druid项目修改了再放开
				} else {
					parserSingleInsert(schema, rrs, partitionColumn, tableName, insert);
				}
				
			}
		}
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
			if(partitionColumn.equalsIgnoreCase(removeBackquote(insertStmt.getColumns().get(i).toString()))) {//找到分片字段
				isFound = true;
				String column = removeBackquote(insertStmt.getColumns().get(i).toString());
				
				String value = removeBackquote(insertStmt.getValues().getValues().get(i).toString());
				ctx.addShardingExpr(tableName, column, value);
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
				String column = removeBackquote(opExpr.getLeft().toString().toUpperCase());
				if(column.equals(partitionColumn)) {
					String msg = "partion key can't be updated: " + tableName + " -> " + partitionColumn;
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
			}
		}
	}
	
//	/**
//	 * insert into .... select .... 或insert into table() values (),(),....
//	 * @param schema
//	 * @param rrs
//	 * @param insertStmt
//	 * @throws SQLNonTransientException
//	 */
//	private void parserBatchInsert(SchemaConfig schema, RouteResultset rrs, String partitionColumn, 
//			String tableName, MySqlInsertStatement insertStmt) throws SQLNonTransientException {
//		//insert into table() values (),(),....
//		if(insertStmt.getValuesList().size() > 1) {
//			//字段列数
//			int columnNum = insertStmt.getColumns().size();
//			int shardingColIndex = getSharingColIndex(insertStmt, partitionColumn);
//			if(shardingColIndex == -1) {
//				String msg = "bad insert sql (sharding column:"+ partitionColumn + " not provided," + insertStmt;
//				LOGGER.warn(msg);
//				throw new SQLNonTransientException(msg);
//			} else {
//				List<ValuesClause> valueClauseList = insertStmt.getValuesList();
//				
//				Map<Integer,List<ValuesClause>> nodeValuesMap = new HashMap<Integer,List<ValuesClause>>();
//				TableConfig tableConfig = schema.getTables().get(tableName);
//				AbstractPartionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
//				for(ValuesClause valueClause : valueClauseList) {
//					if(valueClause.getValues().size() != columnNum) {
//						String msg = "bad insert sql columnSize != valueSize:"
//					             + columnNum + " != " + valueClause.getValues().size() 
//					             + "values:" + valueClause;
//						LOGGER.warn(msg);
//						throw new SQLNonTransientException(msg);
//					}
//					String shardingValue = valueClause.getValues().get(shardingColIndex).toString().toUpperCase();
//					Integer nodeIndex = algorithm.calculate(shardingValue);
//					//没找到插入的分片
//					if(nodeIndex == null) {
//						String msg = "can't find any valid datanode :" + tableName 
//								+ " -> " + partitionColumn + " -> " + shardingValue;
//						LOGGER.warn(msg);
//						throw new SQLNonTransientException(msg);
//					}
//					if(nodeValuesMap.get(nodeIndex) == null) {
//						nodeValuesMap.put(nodeIndex, new ArrayList<ValuesClause>());
//					}
//					nodeValuesMap.get(nodeIndex).add(valueClause);
//					System.out.println();
//				}
//				
//				RouteResultsetNode[] nodes = new RouteResultsetNode[nodeValuesMap.size()];
//				int count = 0;
//				for(Map.Entry<Integer,List<ValuesClause>> node : nodeValuesMap.entrySet()) {
//					Integer nodeIndex = node.getKey();
//					List<ValuesClause> valuesList = node.getValue();
//					insertStmt.setValuesList(valuesList);
//					nodes[count++] = new RouteResultsetNode(tableConfig.getDataNodes().get(nodeIndex),
//							rrs.getSqlType(),insertStmt.toString());
//				}
//				rrs.setNodes(nodes);
//				rrs.setFinishedRoute(true);
//			}
//		} else if(insertStmt.getQuery() != null) { // insert into .... select ....
//			String msg = "TODO:insert into .... select .... not supported!";
//			LOGGER.warn(msg);
//			throw new SQLNonTransientException(msg);
//		}
//	}
//	
//	/**
//	 * 寻找拆分字段在 columnList中的索引
//	 * @param insertStmt
//	 * @param partitionColumn
//	 * @return
//	 */
//	private int getSharingColIndex(MySqlInsertStatement insertStmt,String partitionColumn) {
//		int shardingColIndex = -1;
//		for(int i = 0; i < insertStmt.getColumns().size(); i++) {
//			if(partitionColumn.equalsIgnoreCase(removeBackquote(insertStmt.getColumns().get(i).toString()))) {//找到分片字段
//				shardingColIndex = i;
//				return shardingColIndex;
//			}
//		}
//		return shardingColIndex;
//		
//	}
}
