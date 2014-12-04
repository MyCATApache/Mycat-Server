package org.opencloudb.parser.fdb;

import java.sql.SQLSyntaxErrorException;
import java.util.Collection;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;

import com.foundationdb.sql.parser.QueryTreeNode;

/**
 * fdb parser解析策略接口
 * @author wang.dw
 *
 */
public interface FdbStrategy {
	/**
	 * fdb parser路由到多节点
	 * @param schema
	 * @param isSelect
	 * @param cache
	 * @param ast
	 * @param rrs
	 * @param dataNodes
	 * @param stmt
	 * @return
	 * @throws SQLSyntaxErrorException
	 */
	public abstract RouteResultset routeToMultiNode(SchemaConfig schema,
			boolean isSelect, boolean cache, QueryTreeNode ast,
			RouteResultset rrs, Collection<String> dataNodes, String stmt)
			throws SQLSyntaxErrorException;
}
