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
package org.opencloudb.mpp;

import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.route.RouteResultset;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.AggregateNode;
import com.foundationdb.sql.parser.AndNode;
import com.foundationdb.sql.parser.BetweenOperatorNode;
import com.foundationdb.sql.parser.BinaryOperatorNode;
import com.foundationdb.sql.parser.BinaryRelationalOperatorNode;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.FromBaseTable;
import com.foundationdb.sql.parser.FromList;
import com.foundationdb.sql.parser.FromSubquery;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.GroupByList;
import com.foundationdb.sql.parser.InListOperatorNode;
import com.foundationdb.sql.parser.JoinNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.NumericConstantNode;
import com.foundationdb.sql.parser.OrNode;
import com.foundationdb.sql.parser.OrderByColumn;
import com.foundationdb.sql.parser.OrderByList;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.ResultSetNode;
import com.foundationdb.sql.parser.RowConstructorNode;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.SubqueryNode;
import com.foundationdb.sql.parser.UnaryLogicalOperatorNode;
import com.foundationdb.sql.parser.UnionNode;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.parser.ValueNodeList;
import com.foundationdb.sql.unparser.NodeToString;

public class SelectSQLAnalyser {
	private static final Logger LOGGER = Logger
			.getLogger(SelectSQLAnalyser.class);

	private static String andTableName(SelectParseInf parsInf,
			FromBaseTable fromTable) throws StandardException {
		String tableName = fromTable.getOrigTableName().getTableName();
		String aliasName = fromTable.getTableName().getTableName();
		if ((aliasName != null) && !aliasName.equals(tableName)) {// store alias
			// ->real
			// name
			// relation
			parsInf.ctx.tableAliasMap.put(aliasName, tableName.toUpperCase());
		}
		tableName = tableName.toUpperCase();
		Map<String, Set<ColumnRoutePair>> columVarMap = parsInf.ctx.tablesAndConditions
				.get(tableName);
		if (columVarMap == null) {
			columVarMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
			parsInf.ctx.tablesAndConditions.put(tableName, columVarMap);
		}
		return tableName;
	}

	public static String addLimitCondtionForSelectSQL(RouteResultset rrs,
			CursorNode cursNode, int defaultMaxLimit)
			throws SQLSyntaxErrorException {
		NumericConstantNode offCountNode = new NumericConstantNode();
		offCountNode.setNodeType(NodeTypes.INT_CONSTANT_NODE);
		offCountNode.setValue(defaultMaxLimit);
		cursNode.init(cursNode.statementToString(),
				cursNode.getResultSetNode(), cursNode.getName(),
				cursNode.getOrderByList(), cursNode.getOffsetClause(),
				offCountNode, cursNode.getUpdateMode(),
				cursNode.getUpdatableColumns());
		rrs.setLimitSize(defaultMaxLimit);
		try {
			return new NodeToString().toString(cursNode);
		} catch (StandardException e) {
			throw new SQLSyntaxErrorException(e);
		}
	}

	/**
	 * anlayse group ,order ,limit condtions
	 * 
	 * @param rrs
	 * @param ast
	 * @throws StandardException
	 * @throws SQLSyntaxErrorException
	 */
	public static String analyseMergeInf(RouteResultset rrs, QueryTreeNode ast,
			boolean modifySQLLimit, int defaultMaxLimit)
			throws SQLSyntaxErrorException {
		CursorNode rsNode = (CursorNode) ast;
		NumericConstantNode offsetNode = null;
		NumericConstantNode offCountNode = null;
		ResultSetNode resultNode = rsNode.getResultSetNode();

		if (resultNode instanceof SelectNode) {
			Map<String, Integer> aggrColumns = new HashMap<String, Integer>();
			boolean hasAggrColumn = false;
			SelectNode selNode = (SelectNode) resultNode;
			ResultColumnList colums = selNode.getResultColumns();
			for (int i = 0; i < colums.size(); i++) {
				ResultColumn col = colums.get(i);
				ValueNode exp = col.getExpression();
				if (exp instanceof AggregateNode) {

					hasAggrColumn = true;
					String colName = col.getName();
					String aggName = ((AggregateNode) exp).getAggregateName();
					if (colName != null) {
						aggrColumns
								.put(colName, MergeCol.getMergeType(aggName));
					}
				}
			}
			if (!aggrColumns.isEmpty()) {
				rrs.setMergeCols(aggrColumns);
			}
			if (hasAggrColumn) {
				rrs.setHasAggrColumn(true);
			}

			GroupByList groupL = selNode.getGroupByList();
			if (groupL != null && !groupL.isEmpty()) {
				String[] groupCols = new String[groupL.size()];
				for (int i = 0; i < groupCols.length; i++) {
					groupCols[i] = groupL.get(i).getColumnName();
				}
				rrs.setGroupByCols(groupCols);
			}
		}

		OrderByList orderBy = rsNode.getOrderByList();
		if (orderBy != null && !orderBy.isEmpty()) {

			// get column and alias map
			Map<String, ResultColumn> nameToColumn = new HashMap<String, ResultColumn>();
			ResultColumnList colums = ((SelectNode) resultNode)
					.getResultColumns();
			for (int j = 0; j < colums.size(); j++) {
				ResultColumn col = colums.get(j);
				ValueNode exp = col.getExpression();
				if (exp != null) {
					nameToColumn.put(exp.getColumnName(), col);
				}
			}
			LinkedHashMap<String, Integer> orderCols = new LinkedHashMap<String, Integer>();

			for (int i = 0; i < orderBy.size(); i++) {
				OrderByColumn orderCol = orderBy.get(i);
				ValueNode orderExp = orderCol.getExpression();
				if (!(orderExp instanceof ColumnReference)) {
					throw new SQLSyntaxErrorException(
							" aggregated column should has a alias in order to be used in order by clause");
				}
				String columnName = orderExp.getColumnName();
				ResultColumn rc = nameToColumn.get(orderExp.getColumnName());
				if (rc != null) {
					columnName = rc.getName();
				}
				orderCols.put(columnName,
						orderCol.isAscending() ? OrderCol.COL_ORDER_TYPE_ASC
								: OrderCol.COL_ORDER_TYPE_DESC);
			}
			rrs.setOrderByCols(orderCols);
		}

		if (rsNode.getOffsetClause() != null) {
			offsetNode = (NumericConstantNode) rsNode.getOffsetClause();
			rrs.setLimitStart(Integer
					.parseInt(offsetNode.getValue().toString()));

		}
		if (rsNode.getFetchFirstClause() != null) {
			offCountNode = (NumericConstantNode) rsNode.getFetchFirstClause();
			rrs.setLimitSize(Integer.parseInt(offCountNode.getValue()
					.toString()));
		}
		// if no limit in sql and defaultMaxLimit not equals -1 ,then and limit
		if ((modifySQLLimit) && (offCountNode == null)
				&& (defaultMaxLimit != -1) && !rrs.hasPrimaryKeyToCache()) {
			return addLimitCondtionForSelectSQL(rrs, rsNode, defaultMaxLimit);

		} else if (modifySQLLimit && offsetNode != null) {
			offsetNode.setValue(0);
			offCountNode.setValue(rrs.getLimitStart() + rrs.getLimitSize());
			try {
				return new NodeToString().toString(ast);
			} catch (StandardException e) {
				throw new SQLSyntaxErrorException(e);
			}
		} else {
			return null;
		}

	}

	public static void analyse(SelectParseInf parsInf, QueryTreeNode ast)
			throws SQLSyntaxErrorException {
		try {
			analyseSQL(parsInf, ast, false);
		} catch (StandardException e) {
			throw new SQLSyntaxErrorException(e);
		}
	}

	private static void addTableName(FromSubquery theSub, SelectParseInf parsInf)
			throws StandardException {
		FromList fromList = ((SelectNode) theSub.getSubquery()).getFromList();
		if (fromList.size() == 1) {
			FromTable fromT = fromList.get(0);
			if (fromT instanceof FromBaseTable) {
				FromBaseTable baseT = ((FromBaseTable) fromT);
				String tableName = baseT.getOrigTableName().getTableName();
				String corrName = theSub.getCorrelationName();
				if (corrName != null) {
					andTableName(parsInf, baseT);
					parsInf.ctx.tableAliasMap.put(corrName,
							tableName.toUpperCase());

				}
			}

		}

	}

	private static void analyseSQL(SelectParseInf parsInf, QueryTreeNode ast,
			boolean notOpt) throws StandardException {
		SelectNode selNode = null;
		switch (ast.getNodeType()) {
		case NodeTypes.CURSOR_NODE: {
			ResultSetNode rsNode = ((CursorNode) ast).getResultSetNode();
			if (rsNode instanceof UnionNode) {
				UnionNode unionNode = (UnionNode) rsNode;
				analyseSQL(parsInf, unionNode.getLeftResultSet(), notOpt);
				analyseSQL(parsInf, unionNode.getRightResultSet(), notOpt);
				return;
			} else if (!(rsNode instanceof SelectNode)) {
				LOGGER.info("ignore not select node "
						+ rsNode.getClass().getCanonicalName());
				return;
			}
			selNode = (SelectNode) rsNode;
			break;
		}
		case NodeTypes.SELECT_NODE: {
			selNode = (SelectNode) ast;
			break;
		}
		case NodeTypes.SUBQUERY_NODE: {
			SubqueryNode subq = (SubqueryNode) ast;
			selNode = (SelectNode) subq.getResultSet();
			break;
		}
		case NodeTypes.FROM_SUBQUERY: {
			FromSubquery subq = (FromSubquery) ast;
			selNode = (SelectNode) subq.getSubquery();
			break;
		}
		case NodeTypes.UNION_NODE: {
			UnionNode unionNode = (UnionNode) ast;
			analyseSQL(parsInf, unionNode.getLeftResultSet(), notOpt);
			analyseSQL(parsInf, unionNode.getRightResultSet(), notOpt);
			return;
		}
		default: {
			LOGGER.info("todo :not select node "
					+ ast.getClass().getCanonicalName());
			return;
		}
		}

		FromList fromList = selNode.getFromList();
		int formSize = fromList.size();
		String defaultTableName = null;
		if (formSize == 1) {
			FromTable fromT = fromList.get(0);
			if (fromT instanceof FromBaseTable) {
				FromBaseTable baseT = ((FromBaseTable) fromT);
				defaultTableName = baseT.getOrigTableName().getTableName();
			} else if (fromT instanceof JoinNode) {
				ResultSetNode leftNode = ((JoinNode) fromT).getLeftResultSet();
				while (leftNode instanceof JoinNode) {
					leftNode = ((JoinNode) leftNode).getLeftResultSet();
				}
				if (leftNode instanceof FromBaseTable) {
					defaultTableName = ((FromBaseTable) leftNode)
							.getOrigTableName().getTableName();
				}
			}

		} else if (formSize > 1) {
			FromTable fromT = fromList.get(0);
			if (fromT instanceof FromBaseTable) {
				FromBaseTable baseT = ((FromBaseTable) fromT);
				defaultTableName = baseT.getOrigTableName().getTableName();
			}
		}

		for (int i = 0; i < formSize; i++) {
			FromTable fromT = fromList.get(i);
			if (fromT instanceof FromBaseTable) {
				andTableName(parsInf, (FromBaseTable) fromT);

			} else if (fromT instanceof JoinNode) {
				anlyseJoinNode(parsInf, notOpt, (JoinNode) fromT);

			} else if (fromT instanceof FromSubquery) {
				analyseSQL(parsInf, ((FromSubquery) fromT).getSubquery(),
						notOpt);
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("todo  parse" + fromT.getClass().toString());
				}

			}
		}

		ValueNode valueNode = selNode.getWhereClause();
		if (valueNode == null) {
			return;
		}
		analyseWhereCondition(parsInf, notOpt, defaultTableName, valueNode);
	}

	private static void anlyseJoinNode(SelectParseInf parsInf, boolean notOpt,
			JoinNode joinNd) throws StandardException {
		// FromSubquery
		ResultSetNode leftNode = joinNd.getLeftResultSet();
		if (leftNode instanceof FromSubquery) {
			FromSubquery theSub = (FromSubquery) leftNode;
			addTableName(theSub, parsInf);
			analyseSQL(parsInf, theSub, notOpt);

		} else if (leftNode instanceof FromBaseTable) {
			andTableName(parsInf, (FromBaseTable) leftNode);
		} else if (leftNode instanceof JoinNode) {
			anlyseJoinNode(parsInf, notOpt, (JoinNode) leftNode);

		}
		ResultSetNode rightNode = joinNd.getRightResultSet();
		if (rightNode instanceof FromSubquery) {
			FromSubquery theSub = (FromSubquery) rightNode;
			addTableName(theSub, parsInf);
			analyseSQL(parsInf, theSub, notOpt);

		} else {
			andTableName(parsInf, (FromBaseTable) rightNode);
		}

		BinaryOperatorNode joinClause = (BinaryOperatorNode) joinNd
				.getJoinClause();
		if (joinClause instanceof BinaryRelationalOperatorNode) {
			BinaryRelationalOperatorNode joinOpt = (BinaryRelationalOperatorNode) joinClause;
			// 以下逻辑是为了添加JoinRel，最终用在路由计算上，见ServerRouterUtil中tryRouteForTables中。
			// 路由计算时根据JoinRel，移除掉非root table
			if (joinOpt.getLeftOperand() instanceof ColumnReference) {// join的on条件为on tableA.a = tableB.b
				addTableJoinInf(parsInf.ctx,
						(ColumnReference) joinOpt.getLeftOperand(),
						(ColumnReference) joinOpt.getRightOperand());
			} else {// join的on条件不是 tableA.a = tableB.b，如：ON INSTR(tableA.a,tableB.b) > 0
				// TODO 条件不是tableA.a = tableB.b类型的一般都是非父子表关系，该情况下暂时没处理
				LOGGER.info("TODO,  not supported parse join condition: \n"
						+ new NodeToString().toString(joinOpt));
			}
		} else if (joinClause instanceof AndNode
				|| joinClause instanceof OrNode) {
			BinaryRelationalOperatorNode joinOpt = (BinaryRelationalOperatorNode) joinClause
					.getLeftOperand();
			joinClause.getLeftOperand();
			addTableJoinInf(parsInf.ctx,
					(ColumnReference) joinOpt.getLeftOperand(),
					(ColumnReference) joinOpt.getRightOperand());
		} else {
			throw new StandardException("can't get join info "
					+ joinNd.toString());
		}

	}

	public static void analyseWhereCondition(SelectParseInf parsInf,
			boolean notOpt, String defaultTableName, ValueNode valueNode)
			throws StandardException {
		// valueNode.treePrint();
		if (valueNode instanceof BinaryOperatorNode) {
			BinaryOperatorNode binRelNode = (BinaryOperatorNode) valueNode;
			ValueNode leftOp = binRelNode.getLeftOperand();
			ValueNode wrightOp = binRelNode.getRightOperand();
			tryColumnCondition(parsInf, defaultTableName,
					binRelNode.getMethodName(), leftOp, wrightOp, notOpt);

		} else if (valueNode instanceof SubqueryNode) {
			analyseSQL(parsInf, valueNode, notOpt);
		} else if (valueNode instanceof UnaryLogicalOperatorNode) {
			UnaryLogicalOperatorNode logicNode = (UnaryLogicalOperatorNode) valueNode;
			boolean theNotOpt = logicNode.getOperator().equals("not") | notOpt;
			analyseWhereCondition(parsInf, theNotOpt, defaultTableName,
					logicNode.getOperand());
		} else if (valueNode instanceof InListOperatorNode) {
			if (notOpt) {
				return;
			}
			InListOperatorNode theOpNode = (InListOperatorNode) valueNode;
			parseIncondition(defaultTableName, theOpNode, parsInf.ctx);
			// addConstCondition(theOpNode.getLeftOperand().getNodeList().get(0),
			// theOpNode.getRightOperandList(), "IN", ctx,
			// defaultTableName, notOpt);
		} else if (valueNode instanceof BetweenOperatorNode) {
			// where条件仅有一个between
			analyseBetween((BetweenOperatorNode) valueNode, defaultTableName,
					parsInf.ctx);
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("todo  parse where cond\r\n "
						+ valueNode.getClass().getCanonicalName());
			}

			// valueNode.treePrint();
		}
	}

	// 分析between操作符
	private static void analyseBetween(BetweenOperatorNode valueNode,
			String defaultTableName, ShardingParseInfo ctx) {
		String columnName = valueNode.getLeftOperand().getColumnName();
		if (columnName != null) {
			String beginValue = String.valueOf(((ConstantNode) valueNode
					.getRightOperandList().get(0)).getValue());
			String endValue = String.valueOf(((ConstantNode) valueNode
					.getRightOperandList().get(1)).getValue());
			RangeValue rv = new RangeValue(beginValue, endValue, RangeValue.EE);
			ctx.addShardingExpr(defaultTableName.toUpperCase(), columnName, rv);
		}
	}

	private static void tryColumnCondition(SelectParseInf parsInf,
			String defaultTableName, String methodName, ValueNode leftOp,
			ValueNode wrightOp, boolean notOpt) throws StandardException {

		// 简单的 column=aaa的情况
		if (leftOp instanceof ColumnReference) {
			addConstCondition(leftOp, wrightOp, methodName, parsInf,
					defaultTableName, notOpt);
			return;
		} else if (wrightOp instanceof ColumnReference) {
			addConstCondition(wrightOp, leftOp, methodName, parsInf,
					defaultTableName, notOpt);
			return;
		}

		// 左边 为(a=b) ,(a>b)
		if (leftOp instanceof BinaryOperatorNode) {
			BinaryOperatorNode theOptNode = (BinaryOperatorNode) leftOp;
			tryColumnCondition(parsInf, defaultTableName,
					theOptNode.getMethodName(), theOptNode.getLeftOperand(),
					theOptNode.getRightOperand(), notOpt);
		} else if (leftOp instanceof InListOperatorNode) {
			InListOperatorNode theOpNode = (InListOperatorNode) leftOp;
			addConstCondition(theOpNode.getLeftOperand().getNodeList().get(0),
					theOpNode.getRightOperandList(), "IN", parsInf,
					defaultTableName, notOpt);
		} else if (leftOp instanceof BetweenOperatorNode) {
			BetweenOperatorNode op = (BetweenOperatorNode) leftOp;
			analyseBetween(op, defaultTableName, parsInf.ctx);
		}

		// 右边 为(a=b) ,(a>b)
		if (wrightOp instanceof BinaryOperatorNode) {
			BinaryOperatorNode theOptNode = (BinaryOperatorNode) wrightOp;
			tryColumnCondition(parsInf, defaultTableName,
					theOptNode.getMethodName(), theOptNode.getLeftOperand(),
					theOptNode.getRightOperand(), notOpt);
		} else if (wrightOp instanceof InListOperatorNode) {
			InListOperatorNode theOpNode = (InListOperatorNode) wrightOp;
			addConstCondition(theOpNode.getLeftOperand().getNodeList().get(0),
					theOpNode.getRightOperandList(), "IN", parsInf,
					defaultTableName, notOpt);
		} else if (wrightOp instanceof BetweenOperatorNode) {
			BetweenOperatorNode op = (BetweenOperatorNode) wrightOp;
			analyseBetween(op, defaultTableName, parsInf.ctx);
		}
	}

	private static String getTableNameForColumn(String defaultTable,
			ColumnReference column, Map<String, String> tableAliasMap) {
		String tableName = column.getTableName();
		if (tableName == null) {
			tableName = defaultTable;
		} else {
			// judge if is alias table name
			String realTableName = tableAliasMap.get(tableName);
			if (realTableName != null) {
				return realTableName;
			}
		}
		return tableName.toUpperCase();
	}

	private static void parseIncondition(String tableName,
			InListOperatorNode theOpNode, ShardingParseInfo ctx) {
		ValueNodeList columnLst = theOpNode.getLeftOperand().getNodeList();
		ValueNodeList columnValLst = theOpNode.getRightOperandList()
				.getNodeList();
		int columnSize = columnLst.size();
		for (int i = 0; i < columnSize; i++) {
			ValueNode columNode = columnLst.get(i);
			if (!(columNode instanceof ColumnReference)) {
				continue;
			}
			ColumnReference columRef = (ColumnReference) columNode;
			tableName = getTableNameForColumn(tableName, columRef,
					ctx.tableAliasMap);
			String colName = columRef.getColumnName();
			final Object[] values = new Object[columnValLst.size()];
			for (int j = 0; j < values.length; j++) {
				ValueNode valNode = columnValLst.get(j);
				if (valNode instanceof ConstantNode) {
					values[j] = ((ConstantNode) valNode).getValue();
				} else {
					// rows
					RowConstructorNode rowConsNode = (RowConstructorNode) valNode;
					values[j] = ((ConstantNode) rowConsNode.getNodeList()
							.get(i)).getValue();

				}

			}
			ctx.addShardingExpr(tableName, colName, values);
		}
	}

	private static void addTableJoinInf(ShardingParseInfo ctx,
			ColumnReference leftColum, ColumnReference rightColm)
			throws StandardException {
		// A.a=B.b
		String leftTable = ctx.getTableName(leftColum.getTableName());
		String rightTale = ctx.getTableName(rightColm.getTableName());
		ctx.addJoin(new JoinRel(leftTable, leftColum.getColumnName(),
				rightTale, rightColm.getColumnName()));

	}

	private static void addConstCondition(ValueNode leftOp, ValueNode wrightOp,
			String method, SelectParseInf parsInf, String defaultTableName,
			boolean notOpt) throws StandardException {
		String upMethod = method.toUpperCase();
		if (notOpt) {
			return;
		}
		// System.out.println("before method name :"+method);
		if (!(upMethod.equals("EQUALS") || upMethod.equals("IN"))) {
			return;
		}
		// System.out.println("after method name :"+method);
		if (wrightOp instanceof ConstantNode) {
			ColumnReference leftColumRef = (ColumnReference) leftOp;
			String tableName = getTableNameForColumn(defaultTableName,
					leftColumRef, parsInf.ctx.tableAliasMap);
			parsInf.ctx.addShardingExpr(tableName, leftOp.getColumnName(),
					((ConstantNode) wrightOp).getValue());

		} else if (wrightOp instanceof RowConstructorNode) {
			if (!(leftOp instanceof ColumnReference)) {
				return;
			}
			String tableName = getTableNameForColumn(defaultTableName,
					(ColumnReference) leftOp, parsInf.ctx.tableAliasMap);
			ValueNodeList valList = ((RowConstructorNode) wrightOp)
					.getNodeList();
			final Object[] values = new Object[valList.size()];
			for (int i = 0; i < values.length; i++) {
				ValueNode valNode = valList.get(i);
				values[i] = ((ConstantNode) valNode).getValue();

			}
			parsInf.ctx.addShardingExpr(tableName, leftOp.getColumnName(),
					values);
		} else if (wrightOp instanceof ColumnReference) {
			ColumnReference wrightRef = (ColumnReference) wrightOp;
			if (leftOp instanceof ConstantNode) {
				String tableName = getTableNameForColumn(defaultTableName,
						wrightRef, parsInf.ctx.tableAliasMap);
				parsInf.ctx.addShardingExpr(tableName, leftOp.getColumnName(),
						((ConstantNode) leftOp).getValue());

			} else if (leftOp instanceof ColumnReference) {

				ColumnReference leftCol = (ColumnReference) leftOp;
				addTableJoinInf(parsInf.ctx, leftCol, wrightRef);

			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("todo ,parse condition: "
							+ leftOp.getClass().getCanonicalName());
				}
			}
		} else if (wrightOp instanceof SubqueryNode) {
			analyseSQL(parsInf, ((SubqueryNode) wrightOp).getResultSet(),
					notOpt);
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("todo ,parse condition: "
						+ wrightOp.getClass().getCanonicalName());
			}
		}

	}
}