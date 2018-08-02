package io.mycat.route.impl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat.Relationship;
import com.google.common.base.Strings;
import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.MiddlerQueryResultHandler;
import io.mycat.backend.mysql.nio.handler.MiddlerResultHandler;
import io.mycat.backend.mysql.nio.handler.SecondHandler;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.function.SlotFunction;
import io.mycat.route.impl.middlerResultStrategy.*;
import io.mycat.route.parser.druid.*;
import io.mycat.route.parser.util.ParseUtil;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;

/**
 * Mycat 的 Druid 路由策略
 */
public class DruidMycatRouteStrategy extends AbstractRouteStrategy {

	public static final Logger LOGGER = LoggerFactory.getLogger(DruidMycatRouteStrategy.class);

	/**
	 * 路由中间结果处理映射集合
	 * key是类，value是路由中间结果处理
	 */
	private static Map<Class<?>,RouteMiddlerReaultHandler> middlerResultHandler = new HashMap<>();

	static{
		// SQL查询表达式 对应 SQL查询中间结果处理
		middlerResultHandler.put(SQLQueryExpr.class, new SQLQueryResultHandler());
		// SQL二元操作表达式 对应 SQL二元操作中间结果处理
		// TODO 需要升级druid到1.1.3或更新版本
		// SQLBinaryOpExpr有死循环问题 https://github.com/alibaba/druid/issues/1963 需要升级druid到1.1.3就解决了
		middlerResultHandler.put(SQLBinaryOpExpr.class, new BinaryOpResultHandler());
		// SQL子查询表达式 对应 子查询中间结果处理
		middlerResultHandler.put(SQLInSubQueryExpr.class, new InSubQueryResultHandler());
		// SQL Exists表达式 对应 SQL Exists中间结果处理
		middlerResultHandler.put(SQLExistsExpr.class, new SQLExistsResultHandler());
		// SQL ALL 表达式 对应 SQL ALL中间结果处理
		middlerResultHandler.put(SQLAllExpr.class, new SQLAllResultHandler());
	}

	/**
	 * 通过解析AST语法树类来寻找路由
	 * @param schema 逻辑库配置
	 * @param stmt sql语句
	 * @param rrs 路由结果集
	 * @param charset 字符编码
	 * @param cachePool 缓存池
	 * @param sqlType sql类型
	 * @param sc 前端的服务器连接
	 * @return 路由结果集
	 * @throws SQLNonTransientException
	 */
	@Override
	public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
												String stmt, RouteResultset rrs,String charset,
												LayerCachePool cachePool,int sqlType,ServerConnection sc) throws SQLNonTransientException {
		/**
		 * 只有mysql时只支持mysql语法
		 */
		SQLStatementParser parser = null;
		if (schema.isNeedSupportMultiDBType()) {
			parser = new MycatStatementParser(stmt);
		} else {
			parser = new MySqlStatementParser(stmt);
		}

		MycatSchemaStatVisitor visitor = null;
		SQLStatement statement;

		/**
		 * 解析出现问题统一抛SQL语法错误
		 */
		try {
			// 解析语句。 使用Parser解析生成AST，这里 SQLStatement 就是AST
			statement = parser.parseStatement();
			// 使用 Visitor 访问 AST
			visitor = new MycatSchemaStatVisitor();
		} catch (Exception t) {
			LOGGER.error("DruidMycatRouteStrategyError", t);
			throw new SQLSyntaxErrorException(t);
		}

		/**
		 * 检验不支持的语句
		 */
		checkUnSupportedStatement(statement);

		// 根据数据库类型、SQL语句创建对应的Druid解析器
		DruidParser druidParser = DruidParserFactory.create(schema, statement, visitor);
		// 执行解析
		druidParser.parser(schema, rrs, statement, stmt, cachePool, visitor);
		// 获取解析到的信息
		DruidShardingParseInfo ctx =  druidParser.getCtx() ;
		rrs.setTables(ctx.getTables());//设置相关表

		if(visitor.isSubqueryRelationOr()){
			// 在子查询中，不支持 or 条件。
			String err = "In subQuery,the or condition is not supported.";
			LOGGER.error(err);
			throw new SQLSyntaxErrorException(err);
		}

		/*
		按照以下情况路由
			1.2.1 可以直接路由.
       		1.2.2 两个表夸库join的sql.调用calat
       		1.2.3 需要先执行子查询的sql.把子查询拆分出来.获取结果后,与outerquery
		 */

		//add huangyiming 分片规则不一样的且表中带查询条件的则走Catlet
		List<String> tables = ctx.getTables();
		SchemaConfig schemaConf = MycatServer.getInstance().getConfig().getSchemas().get(schema.getName());
		Set<String> firstDataNodes = new HashSet<String>();
		Map<String, TableConfig> tconfigs = schemaConf==null ? null : schemaConf.getTables(); // 获取数据库所有的表配置

		// 是否可以直接路由标志
		boolean directRoute = true;
		// 表名与路由配置映射，key是表名，value是路由配置
		Map<String,RuleConfig> rulemap = new HashMap<>();
		if(tconfigs!=null){
			int index = 0;
			RuleConfig firstRule = null;
			for(String tableName : tables){
				TableConfig tc =  tconfigs.get(tableName); // 获取slq中表的配置
//				if(tc == null){
//					//add 别名中取
//					Map<String, String> tableAliasMap = ctx.getTableAliasMap();
//					if(tableAliasMap !=null && tableAliasMap.get(tableName) !=null){
//						tc = schemaConf.getTables().get(tableAliasMap.get(tableName));
//					}
//				}
				if(index == 0){
					if(tc !=null){
						firstRule = tc.getRule();
						if(firstRule==null){
							//没有指定分片规则时,不做处理
							continue;
						}
						firstDataNodes.addAll(tc.getDataNodes());
						rulemap.put(tc.getName(), firstRule);
					}
				}else{
					if(tc !=null){
						//ER关系表的时候是可能存在字表中没有tablerule的情况,所以加上判断
						RuleConfig ruleCfg = tc.getRule();
						if(ruleCfg==null){
							//没有指定分片规则时,不做处理
							continue;
						}
						Set<String> dataNodes = new HashSet<String>();
						dataNodes.addAll(tc.getDataNodes());
						rulemap.put(tc.getName(), ruleCfg);
						//如果匹配规则不相同或者分片的datanode不相同则需要走子查询处理
						if(firstRule!=null
								&& (
										( ruleCfg !=null && !ruleCfg.getRuleAlgorithm().equals(firstRule.getRuleAlgorithm())) /* 路由算法不一样 */
												|| (!dataNodes.equals(firstDataNodes)) /* 分片节点不一样 */
									)
								){
							// 不能直接路由
							directRoute = false;
							break;
						}
					}
				}
				index++;
			}
		}

		RouteResultset rrsResult = rrs;
		if(directRoute){ //直接路由
			if(!RouterUtil.isAllGlobalTable(ctx, schemaConf)){
				// SQL语句中含有非全局表
				if(rulemap.size()>1 && !checkRuleField(rulemap,visitor)){
					// 在分片表的情况下，关系条件中没有分片键！
					String err = "In case of slice table,there is no rule field in the relationship condition!";
					LOGGER.error(err);
					throw new SQLSyntaxErrorException(err);
				}
			}
			rrsResult = directRoute(rrs,ctx,schema,druidParser,statement,cachePool);
		}else{
			int subQuerySize = visitor.getSubQuerys().size();
			if(subQuerySize==0 && ctx.getTables().size()==2){
				//没有子查询 两表关联,考虑使用catlet
				if(!visitor.getRelationships().isEmpty()){
					// 存在关系
					rrs.setCacheAble(false);
					rrs.setFinishedRoute(true);
					rrsResult = catletRoute(schema,ctx.getSql(),charset,sc);
				}else{
					// 没有关系
					rrsResult = directRoute(rrs,ctx,schema,druidParser,statement,cachePool);
				}
			}else if(subQuerySize==1){
				//只涉及一张表的子查询,使用  MiddlerResultHandler 获取中间结果后,改写原有 sql 继续执行
				// TODO 后期可能会考虑多个子查询的情况.
				SQLSelect subSqlselect = visitor.getSubQuerys().iterator().next();
				if(!visitor.getRelationships().isEmpty()){
					// 存在关系
					// 当 inner query  和 outer  query  有关联条件时,暂不支持
					String err = "In case of slice table,sql have different rules,the relationship condition is not supported.";
					LOGGER.error(err);
					throw new SQLSyntaxErrorException(err);
				}else{
					SQLSelectQuery subSqlSelectQuery = subSqlselect.getQuery();
					if(((MySqlSelectQueryBlock)subSqlSelectQuery).getFrom() instanceof SQLExprTableSource) {
						// 如 select id,name from (select id,name,age from user) temp 中的 select id,name from user
						rrs.setCacheAble(false);
						rrs.setFinishedRoute(true);
						// 中间结果路由
						rrsResult = middlerResultRoute(schema,charset,subSqlselect,sqlType,statement,sc);
					}
				}
			}else if(subQuerySize >=2){
				// TODO 后期可能会考虑多个子查询的情况.
				//在切片表的情况下，sql有不同的规则，目前只支持一个子查询。
				String err = "In case of slice table,sql has different rules,currently only one subQuery is supported.";
				LOGGER.error(err);
				throw new SQLSyntaxErrorException(err);
			}
		}
		return rrsResult;
	}

	/**
	 * 子查询中存在关联查询的情况下,检查关联字段是否是分片字段
	 * @param rulemap
	 * @param visitor
	 * @return
	 */
	private boolean checkRuleField(Map<String,RuleConfig> rulemap, MycatSchemaStatVisitor visitor){
		if(!MycatServer.getInstance().getConfig().getSystem().isSubqueryRelationshipCheck()){ // 是否检测子查询关系
			return true;
		}
		Set<Relationship> ships = visitor.getRelationships(); // 关系
		Iterator<Relationship> iter = ships.iterator();
		while(iter.hasNext()){
			Relationship ship = iter.next(); // 关系
			String lefttable = ship.getLeft().getTable().toUpperCase(); // 关系中左边表
			String righttable = ship.getRight().getTable().toUpperCase(); // 关系中右边表
			// 如果是同一个表中的关联条件,不做处理
			if(lefttable.equals(righttable)){
				return true;
			}
			RuleConfig leftconfig = rulemap.get(lefttable); // 关系中左边表分片规则
			RuleConfig rightconfig = rulemap.get(righttable); // 关系中右边表分片规则

			if(null!=leftconfig && null!=rightconfig
					&& leftconfig.equals(rightconfig)
					&& leftconfig.getColumn().equals(ship.getLeft().getName().toUpperCase())
					&& rightconfig.getColumn().equals(ship.getRight().getName().toUpperCase())){
				// 左右表的分配规则相同，分片规则中的字段相同
				return true;
			}
		}
		return false;
	}

	/**
	 * 中间结果路由
	 * @param schema
	 * @param charset
	 * @param sqlselect
	 * @param sqlType
	 * @param statement
	 * @param sc
	 * @return
	 */
	private RouteResultset middlerResultRoute(final SchemaConfig schema,final String charset,final SQLSelect sqlselect,
											  final int sqlType,final SQLStatement statement,final ServerConnection sc){

		final String middlesql = SQLUtils.toMySqlString(sqlselect); // 转为MySQL

		MiddlerResultHandler<String> middlerResultHandler = new MiddlerQueryResultHandler<>(new SecondHandler() {
			@Override
			public void doExecute(List param) {
				sc.getSession2().setMiddlerResultHandler(null);
				String sqls = null;
				// 路由计算
				RouteResultset rrs = null;
				try {
					sqls = buildSql(statement,sqlselect,param);
					rrs = MycatServer
							.getInstance()
							.getRouterservice()
							.route(MycatServer.getInstance().getConfig().getSystem(),
									schema, sqlType,sqls.toLowerCase(), charset,sc );

				} catch (Exception e) {
					StringBuilder s = new StringBuilder();
					LOGGER.warn(s.append(this).append(sqls).toString() + " err:" + e.toString(),e);
					String msg = e.getMessage();
					sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
					return;
				}
				NonBlockingSession noBlockSession =  new NonBlockingSession(sc.getSession2().getSource());
				noBlockSession.setMiddlerResultHandler(null);
				//session的预编译标示传递
				noBlockSession.setPrepared(sc.getSession2().isPrepared());
				if (rrs != null) {
					noBlockSession.setCanClose(false);
					noBlockSession.execute(rrs, ServerParse.SELECT);
				}
			}
		} );
		sc.getSession2().setMiddlerResultHandler(middlerResultHandler);
		sc.getSession2().setCanClose(false);

		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, ServerParse.SELECT, middlesql, charset, sc);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(middlesql).toString() + " err:" + e.toString(),e);
			String msg = e.getMessage();
			sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return null;
		}

		if(rrs!=null){
			rrs.setCacheAble(false);
		}
		return rrs;
	}

	/**
	 * 获取子查询执行结果后,改写原始sql 继续执行.
	 * @param statement
	 * @param sqlselect
	 * @param param
	 * @return
	 */
	private String buildSql(SQLStatement statement,SQLSelect sqlselect,List param){
		SQLObject parent = sqlselect.getParent();
		RouteMiddlerReaultHandler handler = middlerResultHandler.get(parent.getClass());
		if(handler==null){
			throw new UnsupportedOperationException(parent.getClass()+" current is not supported ");
		}
		return handler.dohandler(statement, sqlselect, parent, param);
	}

	/**
	 * 两个表的情况，catlet
	 * @param schema
	 * @param stmt
	 * @param charset
	 * @param sc
	 * @return
	 */
	private RouteResultset catletRoute(SchemaConfig schema,String stmt,String charset,ServerConnection sc){
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, ServerParse.SELECT, "/*!mycat:catlet=io.mycat.catlets.ShareJoin */ "+stmt, charset, sc);

		}catch(Exception e){

		}
		return rrs;
	}

	/**
	 * 直接结果路由
	 * @param rrs
	 * @param ctx
	 * @param schema
	 * @param druidParser
	 * @param statement
	 * @param cachePool
	 * @return
	 * @throws SQLNonTransientException
	 */
	private RouteResultset directRoute(RouteResultset rrs,DruidShardingParseInfo ctx,SchemaConfig schema,
									   DruidParser druidParser,SQLStatement statement,LayerCachePool cachePool) throws SQLNonTransientException{

		//改写sql：如insert语句主键自增长, 在直接结果路由的情况下,进行sql 改写处理
		druidParser.changeSql(schema, rrs, statement,cachePool);

		/**
		 * DruidParser 解析过程中已完成了路由的直接返回
		 */
		if ( rrs.isFinishedRoute() ) {
			return rrs;
		}

		/**
		 * 没有from的select语句或其他
		 */
		if((ctx.getTables() == null || ctx.getTables().size() == 0)) {
			return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), druidParser.getCtx().getSql());
		}

		if(druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
		}

		SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
		boolean isAllGlobalTable = RouterUtil.isAllGlobalTable(ctx, schema);
		for(RouteCalculateUnit unit: druidParser.getCtx().getRouteCalculateUnits()) {
			RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool);
			if(rrsTmp != null && rrsTmp.getNodes()!=null) {
				for(RouteResultsetNode node :rrsTmp.getNodes()) {
					nodeSet.add(node);
				}
			}
			if(isAllGlobalTable) {//都是全局表时只计算一遍路由
				break;
			}
		}

		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
		int i = 0;
		for (RouteResultsetNode aNodeSet : nodeSet) {
			nodes[i] = aNodeSet;
			if(statement instanceof MySqlInsertStatement
					&& ctx.getTables().size()==1
					&& schema.getTables().containsKey(ctx.getTables().get(0))) {
				RuleConfig rule = schema.getTables().get(ctx.getTables().get(0)).getRule();
				if(rule!=null&&  rule.getRuleAlgorithm() instanceof SlotFunction){
					aNodeSet.setStatement(ParseUtil.changeInsertAddSlot(aNodeSet.getStatement(),aNodeSet.getSlot()));
				}
			}
			i++;
		}
		rrs.setNodes(nodes);

		//分表
		/**
		 *  subTables="t_order$1-2,t_order3"
		 *目前分表 1.6 开始支持 幵丏 dataNode 在分表条件下只能配置一个，分表条件下不支持join。
		 */
		if(rrs.isDistTable()){ // 路由结果集中是否有不同表
			return this.routeDisTable(statement,rrs);
		}
		return rrs;
	}

	/**
	 * 获取不同的表 -- 子表
	 * @param tableSource
	 * @param node
	 * @return
	 * @throws SQLSyntaxErrorException
	 */
	private SQLExprTableSource getDisTable(SQLTableSource tableSource,RouteResultsetNode node) throws SQLSyntaxErrorException{
		if(node.getSubTableName()==null){
			String msg = " sub table not exists for " + node.getName() + " on " + tableSource;
			LOGGER.error("DruidMycatRouteStrategyError " + msg);
			throw new SQLSyntaxErrorException(msg);
		}

		SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
		sqlIdentifierExpr.setParent(tableSource.getParent());
		sqlIdentifierExpr.setName(node.getSubTableName());
		SQLExprTableSource from2 = new SQLExprTableSource(sqlIdentifierExpr);
		return from2;
	}

	/**
	 * 路由不同的表 -- 子表
	 * @param statement
	 * @param rrs
	 * @return 路由结果集
	 * @throws SQLSyntaxErrorException
	 */
	private RouteResultset routeDisTable(SQLStatement statement, RouteResultset rrs) throws SQLSyntaxErrorException{
		SQLTableSource tableSource = null;
		if(statement instanceof SQLInsertStatement) { // SQL Insert 语句
			SQLInsertStatement insertStatement = (SQLInsertStatement) statement;
			tableSource = insertStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				insertStatement.setTableSource(from2);
				node.setStatement(insertStatement.toString());
			}
		}
		if(statement instanceof SQLDeleteStatement) { // SQL Delete 语句
			SQLDeleteStatement deleteStatement = (SQLDeleteStatement) statement;
			tableSource = deleteStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				deleteStatement.setTableSource(from2);
				node.setStatement(deleteStatement.toString());
			}
		}
		if(statement instanceof SQLUpdateStatement) { // SQL Update 语句
			SQLUpdateStatement updateStatement = (SQLUpdateStatement) statement;
			tableSource = updateStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				updateStatement.setTableSource(from2);
				node.setStatement(updateStatement.toString());
			}
		}

		return rrs;
	}

	/**
	 * SELECT 语句
	 */
	private boolean isSelect(SQLStatement statement) {
		if(statement instanceof SQLSelectStatement) {
			return true;
		}
		return false;
	}

	/**
	 * 检验不支持的SQLStatement类型 ：不支持的类型直接抛SQLSyntaxErrorException异常
	 * @param statement
	 * @throws SQLSyntaxErrorException
	 */
	private void checkUnSupportedStatement(SQLStatement statement) throws SQLSyntaxErrorException {
		//不支持replace语句
		if(statement instanceof SQLReplaceStatement) {
			throw new SQLSyntaxErrorException(" ReplaceStatement can't be supported,use insert into ...on duplicate key update... instead ");
		}
	}

	/**
	 * 分析 Show 语句
	 * @param schema 逻辑库配置
	 * @param rrs 路由结果集
	 * @param stmt sql语句
	 * @return 路由结果集
	 * @throws SQLNonTransientException
	 */
	@Override
	public RouteResultset analyseShowSQL(SchemaConfig schema,
										 RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {

		String upStmt = stmt.toUpperCase();
		int tabInd = upStmt.indexOf(" TABLES");
		if (tabInd > 0) {// show tables
			int[] nextPost = RouterUtil.getSpecPos(upStmt, 0);
			if (nextPost[0] > 0) {// remove db info
				int end = RouterUtil.getSpecEndPos(upStmt, tabInd);
				if (upStmt.indexOf(" FULL") > 0) {
					stmt = "SHOW FULL TABLES" + stmt.substring(end);
				} else {
					stmt = "SHOW TABLES" + stmt.substring(end);
				}
			}
			String defaultNode = schema.getDataNode();
			if(!Strings.isNullOrEmpty(defaultNode)) {
				return RouterUtil.routeToSingleNode(rrs, defaultNode, stmt);
			}
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}

		/**
		 *  show index or column
		 */
		int[] indx = RouterUtil.getSpecPos(upStmt, 0);
		if (indx[0] > 0) {
			/**
			 *  has table
			 */
			int[] repPos = { indx[0] + indx[1], 0 };
			String tableName = RouterUtil.getShowTableName(stmt, repPos);
			/**
			 *  IN DB pattern
			 */
			int[] indx2 = RouterUtil.getSpecPos(upStmt, indx[0] + indx[1] + 1);
			if (indx2[0] > 0) {// find LIKE OR WHERE
				repPos[1] = RouterUtil.getSpecEndPos(upStmt, indx2[0] + indx2[1]);

			}
			stmt = stmt.substring(0, indx[0]) + " FROM " + tableName + stmt.substring(repPos[1]);
			RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
			return rrs;

		}

		/**
		 *  show create table tableName
		 */
		int[] createTabInd = RouterUtil.getCreateTablePos(upStmt, 0);
		if (createTabInd[0] > 0) {
			int tableNameIndex = createTabInd[0] + createTabInd[1];
			if (upStmt.length() > tableNameIndex) {
				String tableName = stmt.substring(tableNameIndex).trim();
				int ind2 = tableName.indexOf('.');
				if (ind2 > 0) {
					tableName = tableName.substring(ind2 + 1);
				}
				RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
				return rrs;
			}
		}

		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}


//	/**
//	 * 为一个表进行条件路由
//	 * @param schema
//	 * @param tablesAndConditions
//	 * @param tablesRouteMap
//	 * @throws SQLNonTransientException
//	 */
//	private static RouteResultset findRouteWithcConditionsForOneTable(SchemaConfig schema, RouteResultset rrs,
//		Map<String, Set<ColumnRoutePair>> conditions, String tableName, String sql) throws SQLNonTransientException {
//		boolean cache = rrs.isCacheAble();
//	    //为分库表找路由
//		tableName = tableName.toUpperCase();
//		TableConfig tableConfig = schema.getTables().get(tableName);
//		//全局表或者不分库的表略过（全局表后面再计算）
//		if(tableConfig.isGlobalTable()) {
//			return null;
//		} else {//非全局表
//			Set<String> routeSet = new HashSet<String>();
//			String joinKey = tableConfig.getJoinKey();
//			for(Map.Entry<String, Set<ColumnRoutePair>> condition : conditions.entrySet()) {
//				String colName = condition.getKey();
//				//条件字段是拆分字段
//				if(colName.equals(tableConfig.getPartitionColumn())) {
//					Set<ColumnRoutePair> columnPairs = condition.getValue();
//
//					for(ColumnRoutePair pair : columnPairs) {
//						if(pair.colValue != null) {
//							Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
//							if(nodeIndex == null) {
//								String msg = "can't find any valid datanode :" + tableConfig.getName()
//										+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
//								LOGGER.warn(msg);
//								throw new SQLNonTransientException(msg);
//							}
//							String node = tableConfig.getDataNodes().get(nodeIndex);
//							if(node != null) {//找到一个路由节点
//								routeSet.add(node);
//							}
//						}
//						if(pair.rangeValue != null) {
//							Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm()
//									.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
//							for(Integer idx : nodeIndexs) {
//								String node = tableConfig.getDataNodes().get(idx);
//								if(node != null) {//找到一个路由节点
//									routeSet.add(node);
//								}
//							}
//						}
//					}
//				} else if(joinKey != null && joinKey.equals(colName)) {
//					Set<String> dataNodeSet = RouterUtil.ruleCalculate(
//							tableConfig.getParentTC(), condition.getValue());
//					if (dataNodeSet.isEmpty()) {
//						throw new SQLNonTransientException(
//								"parent key can't find any valid datanode ");
//					}
//					if (LOGGER.isDebugEnabled()) {
//						LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
//								+ Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
//					}
//					if (dataNodeSet.size() > 1) {
//						return RouterUtil.routeToMultiNode(rrs.isCacheAble(), rrs, schema.getAllDataNodes(), sql);
//					} else {
//						rrs.setCacheAble(true);
//						return RouterUtil.routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
//					}
//				} else {//条件字段不是拆分字段也不是join字段,略过
//					continue;
//
//				}
//			}
//			return RouterUtil.routeToMultiNode(cache, rrs, routeSet, sql);
//
//		}
//
//	}

	/**
	 * 路由信息指令, 如 SHOW、SELECT@@、DESCRIBE
	 * @param schema 逻辑库配置
	 * @param sqlType sql类型
	 * @param stmt sql语句
	 * @param rrs 路由结果集
	 * @return 路由结果集
	 * @throws SQLSyntaxErrorException
	 */
	@Override
	public RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType,
										  String stmt, RouteResultset rrs) throws SQLSyntaxErrorException {
		switch(sqlType){
			case ServerParse.SHOW:
				// if origSQL is like show tables
				// 分析 Show 语句
				return analyseShowSQL(schema, rrs, stmt);
			case ServerParse.SELECT:
				//if origSQL is like select @@
				// 分析双@（@@）标志
				int index = stmt.indexOf("@@");
				if(index > 0 && "SELECT".equals(stmt.substring(0, index).trim().toUpperCase())){
					return analyseDoubleAtSign(schema, rrs, stmt);
				}
				break;
			case ServerParse.DESCRIBE:
				// if origSQL is meta SQL, such as describe table
				// 分析 describe 语句
				int ind = stmt.indexOf(' ');
				stmt = stmt.trim();
				return analyseDescrSQL(schema, rrs, stmt, ind + 1);
		}
		return null;
	}

	/**
	 * // 分析 describe 语句
	 * 对Desc语句进行分析 返回数据路由集合
	 *
	 * @param schema   				数据库名
	 * @param rrs    				数据路由集合
	 * @param stmt   				执行语句
	 * @param ind    				第一个' '的位置
	 * @return RouteResultset		(数据路由集合)
	 * @author mycat
	 */
	private static RouteResultset analyseDescrSQL(SchemaConfig schema,
												  RouteResultset rrs, String stmt, int ind) {
		final String MATCHED_FEATURE = "DESCRIBE ";
		final String MATCHED2_FEATURE = "DESC ";
		int pos = 0;
		while (pos < stmt.length()) {
			char ch = stmt.charAt(pos);
			// 忽略处理注释 /* */ BEN
			if(ch == '/' &&  pos+4 < stmt.length() && stmt.charAt(pos+1) == '*') {
				if(stmt.substring(pos+2).indexOf("*/") != -1) {
					pos += stmt.substring(pos+2).indexOf("*/")+4;
					continue;
				} else {
					// 不应该发生这类情况。
					throw new IllegalArgumentException("sql 注释 语法错误");
				}
			} else if(ch == 'D'||ch == 'd') {
				// 匹配 [describe ]
				if(pos+MATCHED_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED_FEATURE) != -1)) {
					pos = pos + MATCHED_FEATURE.length();
					break;
				} else if(pos+MATCHED2_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED2_FEATURE) != -1)) {
					pos = pos + MATCHED2_FEATURE.length();
					break;
				} else {
					pos++;
				}
			} else {
				pos++;
			}
		}

		// 重置ind坐标。BEN GONG
		ind = pos;
		int[] repPos = { ind, 0 };
		String tableName = RouterUtil.getTableName(stmt, repPos);

		stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
		RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
		return rrs;
	}

	/**
	 * 分析双@（@@）标志
	 * 根据执行语句判断数据路由
	 *
	 * @param schema     			数据库名
	 * @param rrs		  		 	数据路由集合
	 * @param stmt		  	 		执行sql
	 * @return RouteResultset		数据路由集合
	 * @throws SQLSyntaxErrorException
	 * @author mycat
	 */
	private RouteResultset analyseDoubleAtSign(SchemaConfig schema,
											   RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();
		int atSginInd = upStmt.indexOf(" @@");
		if (atSginInd > 0) {
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}
		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}
}