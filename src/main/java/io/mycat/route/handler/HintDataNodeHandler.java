package io.mycat.route.handler;

import java.sql.SQLNonTransientException;
import java.util.Map;

import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.route.parser.druid.MycatStatementParser;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.LoadData;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.ServerConnection;

/**
 * 处理注释中类型为datanode 的情况
 * 
 * @author zhuam
 */
public class HintDataNodeHandler implements HintHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HintSchemaHandler.class);

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema, int sqlType, String realSQL,
			String charset, ServerConnection sc, LayerCachePool cachePool, String hintSQLValue,int hintSqlType, Map hintMap)
					throws SQLNonTransientException {
		
		String stmt = realSQL;
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("route datanode sql hint from " + stmt);
		}
		
		RouteResultset rrs = new RouteResultset(stmt, sqlType);		
		PhysicalDBNode dataNode = MycatServer.getInstance().getConfig().getDataNodes().get(hintSQLValue);
		if (dataNode != null) {			
			rrs = RouterUtil.routeToSingleNode(rrs, dataNode.getName(), stmt);
		} else {
			String msg = "can't find hint datanode:" + hintSQLValue;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		// 处理导入参数初始化
		if(rrs.getSqlType() == ServerParse.LOAD_DATA_INFILE_SQL){
			LOGGER.info("load data use annotation datanode");
			rrs.getNodes()[0].setLoadData(parseLoadDataPram(stmt , charset));
		}
		
		return rrs;
	}

	// 初始化导入参数
	private LoadData parseLoadDataPram(String sql , String connectionCharset)
	{
		SQLStatementParser parser = new MycatStatementParser(sql);
		MySqlLoadDataInFileStatement statement = (MySqlLoadDataInFileStatement) parser.parseStatement();

		LoadData loadData = new LoadData();
		SQLTextLiteralExpr rawLineEnd = (SQLTextLiteralExpr) statement.getLinesTerminatedBy();
		String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.getText();
		loadData.setLineTerminatedBy(lineTerminatedBy);

		SQLTextLiteralExpr rawFieldEnd = (SQLTextLiteralExpr) statement.getColumnsTerminatedBy();
		String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.getText();
		loadData.setFieldTerminatedBy(fieldTerminatedBy);

		SQLTextLiteralExpr rawEnclosed = (SQLTextLiteralExpr) statement.getColumnsEnclosedBy();
		String enclose = rawEnclosed == null ? null : rawEnclosed.getText();
		loadData.setEnclose(enclose);

		SQLTextLiteralExpr escapseExpr =  (SQLTextLiteralExpr)statement.getColumnsEscaped() ;
		String escapse=escapseExpr==null?"\\":escapseExpr.getText();
		loadData.setEscape(escapse);
		String charset = statement.getCharset() != null ? statement.getCharset() : connectionCharset;
		loadData.setCharset(charset);

		String fileName = parseFileName(sql);
		if(StringUtils.isBlank(fileName)){
			throw new RuntimeException(" file name is null !");
		}

		loadData.setFileName(fileName);

		return loadData ;
	}

	// 处理文件名
	private String parseFileName(String sql)
	{
		if (sql.contains("'"))
		{
			int beginIndex = sql.indexOf("'");
			return sql.substring(beginIndex + 1, sql.indexOf("'", beginIndex + 1));
		} else if (sql.contains("\""))
		{
			int beginIndex = sql.indexOf("\"");
			return sql.substring(beginIndex + 1, sql.indexOf("\"", beginIndex + 1));
		}
		return null;
	}

}
