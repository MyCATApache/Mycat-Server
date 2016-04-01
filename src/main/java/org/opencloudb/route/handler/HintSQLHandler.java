package org.opencloudb.route.handler;

import java.sql.SQLNonTransientException;
import java.sql.Types;
import java.util.*;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLTextLiteralExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.route.*;
import org.opencloudb.route.factory.RouteStrategyFactory;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.ObjectUtil;

/**
 * 处理注释中 类型为sql的情况 （按照 注释中的sql做路由解析，而不是实际的sql）
 */
public class HintSQLHandler implements HintHandler {
	
	private RouteStrategy routeStrategy;
	
	public HintSQLHandler() {
		this.routeStrategy = RouteStrategyFactory.getRouteStrategy();
	}

	@Override
	public RouteResultset route(SystemConfig sysConfig, SchemaConfig schema,
			int sqlType, String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool, String hintSQLValue,int hintSqlType, Map hintMap)
			throws SQLNonTransientException {
		
		RouteResultset rrs = routeStrategy.route(sysConfig, schema, hintSqlType,
				hintSQLValue, charset, sc, cachePool);
		
		// 替换RRS中的SQL执行
		RouteResultsetNode[] oldRsNodes = rrs.getNodes();
		RouteResultsetNode[] newRrsNodes = new RouteResultsetNode[oldRsNodes.length];
		for (int i = 0; i < newRrsNodes.length; i++) {
			newRrsNodes[i] = new RouteResultsetNode(oldRsNodes[i].getName(),
					oldRsNodes[i].getSqlType(), realSQL);
		}
		rrs.setNodes(newRrsNodes);

		// 判断是否为调用存储过程的SQL语句，这里不能用SQL解析器来解析判断是否为CALL语句
		if (ServerParse.CALL == sqlType) {
			rrs.setCallStatement(true);

             Procedure procedure=parseProcedure(realSQL,hintMap);
            rrs.setProcedure(procedure);
        //    String sql=procedure.toChangeCallSql(null);
            String sql=realSQL;
            for (RouteResultsetNode node : rrs.getNodes())
            {
                node.setProcedure(procedure);
                node.setHintMap(hintMap);
                node.setStatement(sql);
            }

		}

		return rrs;
	}



    private   Procedure parseProcedure(String sql,Map hintMap)
    {
        boolean fields = hintMap.containsKey("list_fields");
        boolean isResultList= hintMap != null && ("list".equals(hintMap.get("result_type"))|| fields);
        Procedure procedure=new Procedure();
        procedure.setOriginSql(sql);
        procedure.setResultList(isResultList);

        List<String> sqls= Splitter.on(";").trimResults().splitToList(sql)    ;
        Set<String> outSet=new HashSet<>();
        for (int i = sqls.size() - 1; i >= 0; i--)
        {
            String s = sqls.get(i);
            if(Strings.isNullOrEmpty(s))continue;
            SQLStatementParser parser = new MySqlStatementParser(s);
            SQLStatement statement = parser.parseStatement();
            if(statement instanceof SQLSelectStatement)
            {
                MySqlSelectQueryBlock selectQuery= (MySqlSelectQueryBlock) ((SQLSelectStatement) statement).getSelect().getQuery();
                if(selectQuery!=null)
                {
                    List<SQLSelectItem> selectItems=   selectQuery.getSelectList();
                    for (SQLSelectItem selectItem : selectItems)
                    {
                        String select = selectItem.toString();
                        outSet.add(select) ;
                        procedure.getSelectColumns().add(select);
                    }
                }
               procedure.setSelectSql(s);
            }  else  if(statement instanceof SQLCallStatement)
            {
                SQLCallStatement sqlCallStatement = (SQLCallStatement) statement;
                procedure.setName(sqlCallStatement.getProcedureName().getSimpleName());
                List<SQLExpr> paramterList= sqlCallStatement.getParameters();
                for (int i1 = 0; i1 < paramterList.size(); i1++)
                {
                    SQLExpr sqlExpr = paramterList.get(i1);
                    String pName = sqlExpr.toString();
                    String pType=outSet.contains(pName)? ProcedureParameter.OUT:ProcedureParameter.IN;
                    ProcedureParameter parameter=new ProcedureParameter();
                    parameter.setIndex(i1+1);
                    parameter.setName(pName);
                    parameter.setParameterType(pType);
                    if(pName.startsWith("@"))
                    {
                        procedure.getParamterMap().put(pName, parameter);
                    }   else
                    {
                        procedure.getParamterMap().put(String.valueOf(i1+1), parameter);
                    }

                }
                procedure.setCallSql(s);
            }   else  if(statement instanceof SQLSetStatement)
            {
                procedure.setSetSql(s);
                SQLSetStatement setStatement= (SQLSetStatement) statement;
                List<SQLAssignItem> sets= setStatement.getItems();
                for (SQLAssignItem set : sets)
                {
                    String name=set.getTarget().toString();
                     SQLExpr value=set.getValue();
                    ProcedureParameter parameter = procedure.getParamterMap().get(name);
                    if(parameter!=null)
                    {
                        if (value instanceof SQLIntegerExpr)
                        {
                           parameter.setValue(((SQLIntegerExpr) value).getNumber());
                            parameter.setJdbcType(Types.INTEGER);
                        }  else   if(value instanceof SQLNumberExpr)
                        {
                            parameter.setValue(((SQLNumberExpr) value).getNumber());
                            parameter.setJdbcType(Types.NUMERIC);
                        }
                        else if(value instanceof SQLTextLiteralExpr)
                        {
                            parameter.setValue(((SQLTextLiteralExpr) value).getText());
                            parameter.setJdbcType(Types.VARCHAR);
                        }
                        else
                        if (value instanceof SQLValuableExpr)
                        {
                            parameter.setValue(((SQLValuableExpr) value).getValue());
                            parameter.setJdbcType(Types.VARCHAR);
                        }
                    }
                }
            }

        }
        if(fields)
        {
            String list_fields =(String) hintMap.get("list_fields");
            List<String> listFields = Splitter.on(",").trimResults().splitToList( list_fields);
            for (String field : listFields)
            {
                if(!procedure.getParamterMap().containsKey(field))
                {
                    ProcedureParameter parameter=new ProcedureParameter();
                    parameter.setParameterType(ProcedureParameter.OUT);
                    parameter.setName(field);
                    parameter.setJdbcType(-10);
                    parameter.setIndex(procedure.getParamterMap().size()+1);
                    procedure.getParamterMap().put(field,parameter);
                }
            }
            procedure.getListFields().addAll(listFields);
        }
        return procedure;
    }
}
