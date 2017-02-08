package demo.catlets;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.sequence.handler.IncrSequenceMySQLHandler;
import org.opencloudb.sequence.handler.IncrSequencePropHandler;
import org.opencloudb.sequence.handler.IncrSequenceTimeHandler;
import org.opencloudb.sequence.handler.SequenceHandler;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.sqlengine.Catlet;
import org.opencloudb.sqlengine.EngineCtx;
import org.opencloudb.util.StringUtil;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

/**
 * 执行批量插入sequence Id
 * @author 兵临城下
 * @date 2015/03/20
 */
public class BatchInsertSequence implements Catlet {
	private static final Logger LOGGER = Logger.getLogger(BatchInsertSequence.class);
	
	private RouteResultset rrs;//路由结果集
	private String executeSql;//接收执行处理任务的sql
	private SequenceHandler sequenceHandler;//sequence处理对象
	
	//重新路由使用
	private SystemConfig sysConfig;
	private SchemaConfig schema;
	private int sqltype; 
	private String charset; 
	private ServerConnection sc;
	private LayerCachePool cachePool;

	@Override
	public void processSQL(String sql, EngineCtx ctx) {
		try {
			getRoute(executeSql);
			RouteResultsetNode[] nodes = rrs.getNodes();
			if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
					|| nodes[0].getName().equals("")) {
				ctx.getSession().getSource().writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
						"No dataNode found ,please check tables defined in schema:"
								+ ctx.getSession().getSource().getSchema());
				return;
			} 
			
			sc.getSession().execute(rrs, sqltype);//将路由好的数据执行入库
			
		} catch (Exception e) {
			LOGGER.error("BatchInsertSequence.processSQL(String sql, EngineCtx ctx)",e);
		}
	}

	@Override
	public void route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
			String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool) {
		int rs = ServerParse.parse(realSQL);
		this.sqltype = rs & 0xff;
		this.sysConfig=sysConfig; 
		this.schema=schema;
		this.charset=charset; 
		this.sc=sc;	
		this.cachePool=cachePool;	
		
		try {
			MySqlStatementParser parser = new MySqlStatementParser(realSQL);	 
			SQLStatement statement = parser.parseStatement();
			MySqlInsertStatement insert = (MySqlInsertStatement)statement;
			if(insert.getValuesList()!=null){
				String tableName = StringUtil.getTableName(realSQL).toUpperCase();
				TableConfig tableConfig = schema.getTables().get(tableName);
				String primaryKey = tableConfig.getPrimaryKey();//获得表的主键字段
				
				SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
				sqlIdentifierExpr.setName(primaryKey);
				insert.getColumns().add(sqlIdentifierExpr);
				
				if(sequenceHandler == null){
					int seqHandlerType = MycatServer.getInstance().getConfig().getSystem().getSequnceHandlerType();
					switch(seqHandlerType){
						case SystemConfig.SEQUENCEHANDLER_MYSQLDB:
							sequenceHandler = IncrSequenceMySQLHandler.getInstance();
							break;
						case SystemConfig.SEQUENCEHANDLER_LOCALFILE:
							sequenceHandler = IncrSequencePropHandler.getInstance();
							break;
						case SystemConfig.SEQUENCEHANDLER_LOCAL_TIME:
							sequenceHandler = IncrSequenceTimeHandler.getInstance();
							break;
						default:
							throw new java.lang.IllegalArgumentException("Invalid sequnce handler type "+seqHandlerType);
					}
				}
				
				for(ValuesClause vc : insert.getValuesList()){
					SQLIntegerExpr sqlIntegerExpr = new SQLIntegerExpr();
					long value = sequenceHandler.nextId(tableName.toUpperCase());
					sqlIntegerExpr.setNumber(value);//插入生成的sequence值
					vc.addValue(sqlIntegerExpr);
				}
				
				String insertSql = insert.toString();
				this.executeSql = insertSql;
			}
			
		} catch (Exception e) {
			LOGGER.error("BatchInsertSequence.route(......)",e);
		}
	}
	
	/**
	 * 根据sql获得路由执行结果
	 * @param sql
	 */
	private void getRoute(String sql){
		try {
			rrs =RouteStrategyFactory.getRouteStrategy().route(sysConfig, schema, sqltype,sql,charset, sc, cachePool);
		} catch (Exception e) {
			LOGGER.error("BatchInsertSequence.getRoute(String sql)",e);
		}
	}

}
