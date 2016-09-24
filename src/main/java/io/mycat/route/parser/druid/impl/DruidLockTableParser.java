package io.mycat.route.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement.LockType;

import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.SplitUtil;

/**
 * lock tables [table] [write|read]语句解析器
 * @author songdabin
 */
public class DruidLockTableParser extends DefaultDruidParser implements DruidParser {
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt)
			throws SQLNonTransientException {
		MySqlLockTableStatement lockTableStat = (MySqlLockTableStatement)stmt;
		String table = lockTableStat.getTableSource().toString().toUpperCase();
		TableConfig tableConfig = schema.getTables().get(table);
		if (tableConfig == null) {
			String msg = "can't find table define of " + table + " in schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		LockType lockType = lockTableStat.getLockType();
		if (LockType.WRITE != lockType && LockType.READ != lockType) {
			String msg = "lock type must be write or read";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		List<String> dataNodes = tableConfig.getDataNodes();
		RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
		for (int i = 0; i < dataNodes.size(); i ++) {
			nodes[i] = new RouteResultsetNode(dataNodes.get(i), ServerParse.LOCK, stmt.toString());
		}
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
	}
	
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor)
			throws SQLNonTransientException {
		// 对于lock tables table1 write, table2 read类型的多表锁语句，DruidParser只能解析出table1，
		// 由于多表锁在分布式场景处理逻辑繁琐，且应用场景较少，因此在此处对这种锁表语句进行拦截。
		// 多表锁的语句在语义上会有","，这里以此为判断依据
		String sql = rrs.getStatement();
		sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
		String[] stmts = SplitUtil.split(sql, ',', true);
		// 如果命令中存在","，则按多表锁的语句来处理
		if (stmts.length > 1) {
			String tmpStmt = null;
			String tmpWords[] = null;
			for (int i = 1; i < stmts.length; i ++) {
				tmpStmt = stmts[i];
				tmpWords = SplitUtil.split(tmpStmt, ' ', true);
				if (tmpWords.length==2 && ("READ".equalsIgnoreCase(tmpWords[1]) || "WRITE".equalsIgnoreCase(tmpWords[1]))) {
					// 如果符合多表锁的语法，则继续，并在最后提示不能多表锁！
					continue;
				} else {
					// 如果不符合多表锁的语法，则提示语法错误和不能多表锁！
					throw new SQLNonTransientException("You have an error in your SQL syntax, don't support lock multi tables!");
				}
			}
			LOGGER.error("can't lock multi-table");
			throw new SQLNonTransientException("can't lock multi-table");
		}
	}
}
