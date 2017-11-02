package io.mycat.route.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.mycat.MycatServer;
import io.mycat.cache.LayerCachePool;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.SessionSQLPair;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.server.ErrorCode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.TableConfig;
import io.mycat.server.executors.FetchStoreNodeOfChildTableHandler;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.LoadData;
import io.mycat.util.StringUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * @author wang.dw
 *
 */
public class RouterUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);
    /**
     * 移除执行语句中的数据库名
     *
     * @param stmt
     *            执行语句
     * @param schema
     *            数据库名
     * @return 执行语句
     * @author mycat
     */

    public static String removeSchema(String stmt, String schema) {
        final String upStmt = stmt.toUpperCase();
        final String upSchema = schema.toUpperCase() + ".";
        int strtPos = 0;
        int indx = 0;
        boolean flag = false;
        indx = upStmt.indexOf(upSchema, strtPos);
        if (indx < 0) {
            StringBuilder sb = new StringBuilder("`").append(
                    schema.toUpperCase()).append("`.");
            indx = upStmt.indexOf(sb.toString(), strtPos);
            flag = true;
            if (indx < 0) {
                return stmt;
            }
        }
        StringBuilder sb = new StringBuilder();
        while (indx > 0) {
            sb.append(stmt.substring(strtPos, indx));
            strtPos = indx + upSchema.length();
            if (flag) {
                strtPos += 2;
            }
            indx = upStmt.indexOf(upSchema, strtPos);
        }
        sb.append(stmt.substring(strtPos));
        return sb.toString();
    }

    /**
     * 获取第一个节点作为路由
     *
     * @param rrs
     *            数据路由集合
     * @param dataNode
     *            数据库所在节点
     * @param stmt
     *            执行语句
     * @return 数据路由集合
     * @author mycat
     */
    public static RouteResultset routeToSingleNode(RouteResultset rrs,
                                                   String dataNode, String stmt) {
        if (dataNode == null) {
            return rrs;
        }
        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);//rrs.getStatement()
        rrs.setNodes(nodes);
        rrs.setFinishedRoute(true);
        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if(rrs.getRunOnSlave() != null){
        	nodes[0].setRunOnSlave(rrs.getRunOnSlave());
        }
        return rrs;
    }

    /**
     * 获取table名字
     *
     * @param stmt
     *            执行语句
     * @param repPos
     *            开始位置和位数
     * @return 表名
     * @author AStoneGod
     */
    public static String getTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}
		int thiInd = stmt.indexOf('(',secInd+1);
		if (thiInd < 0) {
			thiInd = stmt.length();
		}
		repPos[1] = secInd;
		String tableName = "";
		if (stmt.toUpperCase().startsWith("DESC")||stmt.toUpperCase().startsWith("DESCRIBE")){
			tableName = stmt.substring(startPos, thiInd).trim();
		}else {
			tableName = stmt.substring(secInd, thiInd).trim();
		}

		//ALTER TABLE
		if (tableName.contains(" ")){
			tableName = tableName.substring(0,tableName.indexOf(" "));
		}
		if (tableName.contains("\n")){
			tableName = tableName.substring(0,tableName.indexOf("\n"));
		}
		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author mycat
     */
    public static int[] getCreateTablePos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

    /**
     * 获取语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author mycat
     */
    public static int[] getSpecPos(String upStmt, int start) {
		String token1 = " FROM ";
		String token2 = " IN ";
		int tabInd1 = upStmt.indexOf(token1, start);
		int tabInd2 = upStmt.indexOf(token2, start);
		if (tabInd1 > 0) {
			if (tabInd2 < 0) {
				return new int[] { tabInd1, token1.length() };
			}
			return (tabInd1 < tabInd2) ? new int[] { tabInd1, token1.length() }
					: new int[] { tabInd2, token2.length() };
		} else {
			return new int[] { tabInd2, token2.length() };
		}
	}

    /**
     * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
     *
     * @param upStmt
     *            执行sql
     * @param start
     *            开发位置
     * @return int
     * @author mycat
     */
    public static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
	}

    public static boolean processWithMycatSeq(SchemaConfig schema, int sqlType,
                                              String origSQL, MySQLFrontConnection sc) {
        // check if origSQL is with global sequence
        // @micmiu it is just a simple judgement
        if (origSQL.indexOf(" MYCATSEQ_") != -1) {
            processSQL(sc,schema,origSQL,sqlType);
            return true;
        }
        return false;
    }

    public static void processSQL(MySQLFrontConnection sc,SchemaConfig schema,String sql,int sqlType){
        MycatServer.getInstance().getSequnceProcessor().addNewSql(new SessionSQLPair(sc.getSession2(), schema, sql, sqlType));
    }

    public static boolean processInsert(SchemaConfig schema, int sqlType,
                                        String origSQL, MySQLFrontConnection sc) throws SQLNonTransientException {
        String tableName = StringUtil.getTableName(origSQL).toUpperCase();
        TableConfig tableConfig = schema.getTables().get(tableName);
        boolean processedInsert=false;
        if (null != tableConfig && tableConfig.isAutoIncrement()) {
            String primaryKey = tableConfig.getPrimaryKey();
            processedInsert=processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey);
        }
        return processedInsert;
    }

    private static boolean isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){
        if(primaryKey==null)
        {
            throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml")  ;
        }
        boolean isPrimaryKeyInFields=false;
        String upperSQL=origSQL.substring(firstLeftBracketIndex,firstRightBracketIndex+1).toUpperCase();
        for(int pkOffset=0,primaryKeyLength=primaryKey.length(),pkStart=0;;){
            pkStart=upperSQL.indexOf(primaryKey, pkOffset);
            if(pkStart>=0 && pkStart<firstRightBracketIndex){
                char pkSide=upperSQL.charAt(pkStart-1);
                if(pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide=='('){
                    pkSide=upperSQL.charAt(pkStart+primaryKey.length());
                    isPrimaryKeyInFields=pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide==')';
                }
                if(isPrimaryKeyInFields){
                    break;
                }
                pkOffset=pkStart+primaryKeyLength;
            }else{
                break;
            }
        }
        return isPrimaryKeyInFields;
    }

    public static boolean processInsert(MySQLFrontConnection sc,SchemaConfig schema,
                                        int sqlType,String origSQL,String tableName,String primaryKey) throws SQLNonTransientException {

        int firstLeftBracketIndex = origSQL.indexOf("(");
        int firstRightBracketIndex = origSQL.indexOf(")");
        String upperSql = origSQL.toUpperCase();
        int valuesIndex = upperSql.indexOf("VALUES");
        int selectIndex = upperSql.indexOf("SELECT");
        int fromIndex = upperSql.indexOf("FROM");
        if(firstLeftBracketIndex < 0) {//insert into table1 select * from table2
            String msg = "invalid sql:" + origSQL;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        if(selectIndex > 0 &&fromIndex>0&&selectIndex>firstRightBracketIndex&&valuesIndex<0) {
            String msg = "multi insert not provided" ;
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }

        if(valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
            throw new SQLSyntaxErrorException("insert must provide ColumnList");
        }

        boolean processedInsert=!isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
        if(processedInsert){
            processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey,firstLeftBracketIndex+1,origSQL.indexOf('(',firstRightBracketIndex)+1);
        }
        return processedInsert;
    }

    private static void processInsert(MySQLFrontConnection sc,SchemaConfig schema,int sqlType,String origSQL,String tableName,String primaryKey,int afterFirstLeftBracketIndex,int afterLastLeftBracketIndex){
        int primaryKeyLength=primaryKey.length();
        int insertSegOffset=afterFirstLeftBracketIndex;
        String mycatSeqPrefix="next value for MYCATSEQ_";
        int mycatSeqPrefixLength=mycatSeqPrefix.length();
        int tableNameLength=tableName.length();

        char[] newSQLBuf=new char[origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+2];
        origSQL.getChars(0, afterFirstLeftBracketIndex, newSQLBuf, 0);
        primaryKey.getChars(0,primaryKeyLength,newSQLBuf,insertSegOffset);
        insertSegOffset+=primaryKeyLength;
        newSQLBuf[insertSegOffset]=',';
        insertSegOffset++;
        origSQL.getChars(afterFirstLeftBracketIndex,afterLastLeftBracketIndex,newSQLBuf,insertSegOffset);
        insertSegOffset+=afterLastLeftBracketIndex-afterFirstLeftBracketIndex;
        mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
        insertSegOffset+=mycatSeqPrefixLength;
        tableName.getChars(0,tableNameLength,newSQLBuf,insertSegOffset);
        insertSegOffset+=tableNameLength;
        newSQLBuf[insertSegOffset]=',';
        insertSegOffset++;
        origSQL.getChars(afterLastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
        processSQL(sc,schema,new String(newSQLBuf),sqlType);
    }


    /**
     * 获取show语句table名字
     *
     * @param stmt
     *            执行语句
     * @param repPos
     *            开始位置和位数
     * @return 表名
     * @author AStoneGod
     */
    public static String getShowTableName(String stmt, int[] repPos) {
        int startPos = repPos[0];
        int secInd = stmt.indexOf(' ', startPos + 1);
        if (secInd < 0) {
            secInd = stmt.length();
        }

        repPos[1] = secInd;
        String tableName = stmt.substring(startPos, secInd).trim();

        int ind2 = tableName.indexOf('.');
        if (ind2 > 0) {
            tableName = tableName.substring(ind2 + 1);
        }
        return tableName;
    }

    /**
     * 处理SQL
     *
     * @param stmt
     *            执行语句
     * @return 处理后SQL
     * @author AStoneGod
     */

    public static String getFixedSql(String stmt){
        if (stmt.endsWith(";"))
            stmt = stmt.substring(0,stmt.length()-2);
        return stmt = stmt.trim().replace("`","");  
    }

    /**
     * 获取ALTER语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getAlterTablePos(String upStmt, int start) {
		String token1 = "ALTER ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

    /**
     * 获取DROP语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getDropTablePos(String upStmt, int start) {
		//增加 if exists判断
		if(upStmt.contains("EXISTS")){
			String token1 = "IF ";
			String token2 = " EXISTS ";
			int ifInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);
			if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}else {
			String token1 = "DROP ";
			String token2 = " TABLE ";
			int createInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);

			if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}
	}


    /**
     * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
     *
     * @param upStmt
     *            执行语句
     * @param start
     *            开始位置
     * @return int[]关键字位置和占位个数
     * @author aStoneGod
     */
    public static int[] getTruncateTablePos(String upStmt, int start) {
		String token1 = "TRUNCATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

    /**
     * 修复DDL路由
     *
     * @return RouteResultset
     * @author aStoneGod
     */
    public static RouteResultset routeToDDLNode(RouteResultset rrs, int sqlType, String stmt,SchemaConfig schema) throws SQLSyntaxErrorException {
    	//检查表是否在配置文件中
		stmt = getFixedSql(stmt);
		String tablename = "";		
		final String upStmt = stmt.toUpperCase();
		if(upStmt.startsWith("CREATE")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getCreateTablePos(upStmt, 0));
		}else if(upStmt.startsWith("DROP")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getDropTablePos(upStmt, 0));
		}else if(upStmt.startsWith("ALTER")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getAlterTablePos(upStmt, 0));
		}else if (upStmt.startsWith("TRUNCATE")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getTruncateTablePos(upStmt, 0));
		}
		tablename = tablename.toUpperCase();
        
        if (schema.getTables().containsKey(tablename)){
            if(ServerParse.DDL==sqlType){
                List<String> dataNodes = new ArrayList<>();
                Map<String, TableConfig> tables = schema.getTables();
                TableConfig tc;
                if (tables != null && (tc = tables.get(tablename)) != null) {
                    dataNodes = tc.getDataNodes();
                }
                Iterator<String> iterator1 = dataNodes.iterator();
                int nodeSize = dataNodes.size();
                RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];

                for(int i=0;i<nodeSize;i++){
                    String name = iterator1.next();
                    nodes[i] = new RouteResultsetNode(name, sqlType, stmt);
                }
                rrs.setNodes(nodes);
            }
            return rrs;
        }else if(schema.getDataNode()!=null){		//默认节点ddl
            RouteResultsetNode[] nodes = new RouteResultsetNode[1];
            nodes[0] = new RouteResultsetNode(schema.getDataNode(), sqlType, stmt);
            rrs.setNodes(nodes);
            return rrs;
        }
        //不在，返回null
        LOGGER.error("table not in schema----"+tablename);
        throw new SQLSyntaxErrorException("op table not in schema----"+tablename);
    }


    public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt) {
        RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
        int i = 0;
        RouteResultsetNode node;
        for (String dataNode : dataNodes) {
            node = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
            if (rrs.getCanRunInReadDB() != null) {
                node.setCanRunInReadDB(rrs.getCanRunInReadDB());
            }
            if(rrs.getRunOnSlave() != null){
            	nodes[0].setRunOnSlave(rrs.getRunOnSlave());
            }
            nodes[i++] = node;
        }
        rrs.setCacheAble(cache);
        rrs.setNodes(nodes);
        return rrs;
    }

    public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt,boolean isGlobalTable) {
        rrs=routeToMultiNode(cache,rrs,dataNodes,stmt);
        rrs.setGlobalTable(isGlobalTable);
        return rrs;
    }

    public static void routeForTableMeta(RouteResultset rrs,
                                         SchemaConfig schema, String tableName, String sql) {
        String dataNode = null;
        if (isNoSharding(schema,tableName)) {//不分库的直接从schema中获取dataNode
            dataNode = schema.getDataNode();
        } else {
            dataNode = getMetaReadDataNode(schema, tableName);
        }

        RouteResultsetNode[] nodes = new RouteResultsetNode[1];
        nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
        if (rrs.getCanRunInReadDB() != null) {
            nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
        }
        if(rrs.getRunOnSlave() != null){
        	nodes[0].setRunOnSlave(rrs.getRunOnSlave());
        }
        rrs.setNodes(nodes);
    }

    /**
     * 根据标名随机获取一个节点
     *
     * @param schema
     *            数据库名
     * @param table
     *            表名
     * @return 数据节点
     * @author mycat
     */
    private static String getMetaReadDataNode(SchemaConfig schema,
                                              String table) {
        // Table名字被转化为大写的，存储在schema
        table = table.toUpperCase();
        String dataNode = null;
        Map<String, TableConfig> tables = schema.getTables();
        TableConfig tc;
        if (tables != null && (tc = tables.get(table)) != null) {
            dataNode = tc.getRandomDataNode();
        }
        return dataNode;
    }

    /**
     * 根据 ER分片规则获取路由集合
     *
     * @param stmt
     *            执行的语句
     * @param rrs
     *            数据路由集合
     * @param tc
     *            表实体
     * @param joinKeyVal
     *            连接属性
     * @return RouteResultset(数据路由集合)
     * @throws SQLNonTransientException
     * @author mycat
     */

    public static RouteResultset routeByERParentKey(MySQLFrontConnection sc,SchemaConfig schema,
                                                    int sqlType,String stmt,
                                                    RouteResultset rrs, TableConfig tc, String joinKeyVal)
            throws SQLNonTransientException {
        // only has one parent level and ER parent key is parent
        // table's partition key
        if (tc.isSecondLevel()
                && tc.getParentTC().getPartitionColumn()
                .equals(tc.getParentKey())) { // using
            // parent
            // rule to
            // find
            // datanode
            Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
            ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
            parentColVal.add(pair);
            Set<String> dataNodeSet = ruleCalculate(tc.getParentTC(),
                    parentColVal);
            if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
                throw new SQLNonTransientException(
                        "parent key can't find  valid datanode ,expect 1 but found: "
                                + dataNodeSet.size());
            }
            String dn = dataNodeSet.iterator().next();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
                        + dn + " sql :" + stmt);
            }
            return RouterUtil.routeToSingleNode(rrs, dn, stmt);
        }
        return null;
    }

    /**
     * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
     */
    public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
                                                       Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {

        String joinValue = "";

        if(colRoutePairSet.size() > 1) {
            LOGGER.warn("joinKey can't have multi Value");
        } else {
            Iterator it = colRoutePairSet.iterator();
            ColumnRoutePair joinCol = (ColumnRoutePair)it.next();
            joinValue = joinCol.colValue;
        }

        Set<String> retNodeSet = new LinkedHashSet<String>();

        Set<String> nodeSet = new LinkedHashSet<String>();
        if (tc.isSecondLevel()
                && tc.getParentTC().getPartitionColumn()
                .equals(tc.getParentKey())) { // using
            // parent
            // rule to
            // find
            // datanode

            nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet);
            if (nodeSet.isEmpty()) {
                throw new SQLNonTransientException(
                        "parent key can't find  valid datanode ,expect 1 but found: "
                                + nodeSet.size());
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
                        + nodeSet + " sql :" + rrs.getStatement());
            }
            retNodeSet.addAll(nodeSet);

//			for(ColumnRoutePair pair : colRoutePairSet) {
//				nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet);
//				if (nodeSet.isEmpty() || nodeSet.size() > 1) {//an exception would be thrown, if sql was executed on more than on sharding
//					throw new SQLNonTransientException(
//							"parent key can't find  valid datanode ,expect 1 but found: "
//									+ nodeSet.size());
//				}
//				String dn = nodeSet.iterator().next();
//				if (LOGGER.isDebugEnabled()) {
//					LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
//							+ dn + " sql :" + rrs.getStatement());
//				}
//				retNodeSet.addAll(nodeSet);
//			}
            return retNodeSet;
        } else {
            retNodeSet.addAll(tc.getParentTC().getDataNodes());
        }

        return retNodeSet;
    }


    /**
     * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
     */
    public static Set<String> ruleCalculate(TableConfig tc,
                                            Set<ColumnRoutePair> colRoutePairSet) {
        Set<String> routeNodeSet = new LinkedHashSet<String>();
        String col = tc.getRule().getColumn();
        RuleConfig rule = tc.getRule();
        AbstractPartitionAlgorithm algorithm = rule.getRuleAlgorithm();
        for (ColumnRoutePair colPair : colRoutePairSet) {
            if (colPair.colValue != null) {
                Integer nodeIndx = algorithm.calculate(colPair.colValue);
                if (nodeIndx == null) {
                    throw new IllegalArgumentException(
                            "can't find datanode for sharding column:" + col
                                    + " val:" + colPair.colValue);
                } else {
                    String dataNode = tc.getDataNodes().get(nodeIndx);
                    routeNodeSet.add(dataNode);
                    colPair.setNodeId(nodeIndx);
                }
            } else if (colPair.rangeValue != null) {
                Integer[] nodeRange = algorithm.calculateRange(
                        String.valueOf(colPair.rangeValue.beginValue),
                        String.valueOf(colPair.rangeValue.endValue));
                if (nodeRange != null) {
                    /**
                     * 不能确认 colPair的 nodeid是否会有其它影响
                     */
                    if (nodeRange.length == 0) {
                        routeNodeSet.addAll(tc.getDataNodes());
                    } else {
                        ArrayList<String> dataNodes = tc.getDataNodes();
                        String dataNode = null;
                        for (Integer nodeId : nodeRange) {
                            dataNode = dataNodes.get(nodeId);
                            routeNodeSet.add(dataNode);
                        }
                    }
                }
            }

        }
        return routeNodeSet;
    }

    /**
     * 多表路由
     * @param schema
     * @param ctx
     * @param tables
     * @param rrs
     * @param isSelect
     * @return
     * @throws SQLNonTransientException
     */
    public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, RouteResultset rrs,
                                                   boolean isSelect, LayerCachePool cachePool) throws SQLNonTransientException {
        List<String> tables = ctx.getTables();
        if(schema.isNoSharding()||(tables.size() >= 1&&isNoSharding(schema,tables.get(0)))) {
            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
        }

        //只有一个表的
        if(tables.size() == 1) {
            return RouterUtil.tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect, cachePool);
        }

        Set<String> retNodesSet = new HashSet<String>();
        //每个表对应的路由映射
        Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();

        //分库解析信息不为空
        Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
        if(tablesAndConditions != null && tablesAndConditions.size() > 0) {
            //为分库表找路由
            RouterUtil.findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), cachePool, isSelect);
            if(rrs.isFinishedRoute()) {
                return rrs;
            }
        }

        //为全局表和单库表找路由
        for(String tableName : tables) {
            TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
            if(tableConfig == null) {
                String msg = "can't find table define in schema "+ tableName + " schema:" + schema.getName();
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            if(tableConfig.isGlobalTable()) {//全局表
                if(tablesRouteMap.get(tableName) == null) {
                    tablesRouteMap.put(tableName, new HashSet<String>());
                }
                tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
            } else if(tablesRouteMap.get(tableName) == null) { //余下的表都是单库表
                tablesRouteMap.put(tableName, new HashSet<String>());
                tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
            }
        }

        boolean isFirstAdd = true;
        for(Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
            if(entry.getValue() == null || entry.getValue().size() == 0) {
                throw new SQLNonTransientException("parent key can't find any valid datanode ");
            } else {
                if(isFirstAdd) {
                    retNodesSet.addAll(entry.getValue());
                    isFirstAdd = false;
                } else {
                    retNodesSet.retainAll(entry.getValue());
                    if(retNodesSet.size() == 0) {//两个表的路由无交集
                        String errMsg = "invalid route in sql, multi tables found but datanode has no intersection "
                                + " sql:" + ctx.getSql();
                        LOGGER.warn(errMsg);
                        throw new SQLNonTransientException(errMsg);
                    }
                }
            }
        }

        if(retNodesSet != null && retNodesSet.size() > 0) {
            if(retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
                // mulit routes ,not cache route result
                if (isSelect) {
                    rrs.setCacheAble(false);
                    routeToSingleNode(rrs, retNodesSet.iterator().next(), ctx.getSql());
                }
                else {//delete 删除全局表的记录
                    routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(),true);
                }

            } else {
                routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
            }

        }
        return rrs;

    }

    /**
     *
     * 单表路由
     * @param schema
     * @param ctx
     * @param tableName
     * @param rrs
     * @param isSelect
     * @return
     * @throws SQLNonTransientException
     */
    public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs,
                                                     boolean isSelect, LayerCachePool cachePool) throws SQLNonTransientException {
        if(isNoSharding(schema,tableName))
        {
            return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
        }

        TableConfig tc = schema.getTables().get(tableName);
        if(tc == null) {
            String msg = "can't find table define in schema "
                    + tableName + " schema:" + schema.getName();
            LOGGER.warn(msg);
            throw new SQLNonTransientException(msg);
        }
        if(tc.isGlobalTable()) {//全局表
            if(isSelect) {
                // global select ,not cache route result
                rrs.setCacheAble(false);
                return routeToSingleNode(rrs, tc.getRandomDataNode(),ctx.getSql());
            } else {//insert into 全局表的记录
                return routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql(),true);
            }
        } else {//单表或者分库表
            if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
                throw new IllegalArgumentException("route rule for table "
                        + tc.getName() + " is required: " + ctx.getSql());

            }
            if(tc.getPartitionColumn() == null && !tc.isSecondLevel()) {//单表且不是childTable
//				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
                return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
            } else {
                //每个表对应的路由映射
                Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
                if(routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
                    RouterUtil.findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, ctx.getSql(),cachePool,isSelect);
                    if(rrs.isFinishedRoute()) {
                        return rrs;
                    }
                }

                if(tablesRouteMap.get(tableName) == null) {
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
                } else {
//					boolean isCache = rrs.isCacheAble();
//					if(tablesRouteMap.get(tableName).size() > 1) {
//
//					}
                    return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName), ctx.getSql());
                }
            }
        }
    }

    /**
     * 处理分库表路由
     * @param schema
     * @param tablesAndConditions
     * @param tablesRouteMap
     * @throws SQLNonTransientException
     */
    public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
                                                         Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
                                                         Map<String, Set<String>> tablesRouteMap, String sql, LayerCachePool cachePool, boolean isSelect)
            throws SQLNonTransientException {
        //为分库表找路由
        for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
            String tableName = entry.getKey().toUpperCase();
            TableConfig tableConfig = schema.getTables().get(tableName);
            if(tableConfig == null) {
                String msg = "can't find table define in schema "
                        + tableName + " schema:" + schema.getName();
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }
            //全局表或者不分库的表略过（全局表后面再计算）
            if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
                continue;
            } else {//非全局表：分库表、childTable、其他
                Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
                String joinKey = tableConfig.getJoinKey();
                String partionCol = tableConfig.getPartitionColumn();
                String primaryKey = tableConfig.getPrimaryKey();
                boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
                boolean isLoadData=false;
                if (LOGGER.isDebugEnabled()) {
                    if(sql.startsWith(LoadData.loadDataHint)||rrs.isLoadData())
                    { //由于load data一次会计算很多路由数据，如果输出此日志会极大降低load data的性能
                        isLoadData=true;
                    }
                }
                if(entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1&&!isLoadData)
                {//主键查找
                    // try by primary key if found in cache
                    Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
                    if (primaryKeyPairs != null) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("try to find cache by primary key ");
                        }
                        String tableKey = schema.getName() + '_' + tableName;
                        boolean allFound = true;
                        for (ColumnRoutePair pair : primaryKeyPairs) {//可能id in(1,2,3)多主键
                            String cacheKey = pair.colValue;
                            String dataNode = (String) cachePool.get(tableKey, cacheKey);
                            if (dataNode == null) {
                                allFound = false;
                                continue;
                            } else {
                                if(tablesRouteMap.get(tableName) == null) {
                                    tablesRouteMap.put(tableName, new HashSet<String>());
                                }
                                tablesRouteMap.get(tableName).add(dataNode);
                                continue;
                            }
                        }
                        if (!allFound) {
                            // need cache primary key ->datanode relation
                            if (isSelect && tableConfig.getPrimaryKey() != null) {
                                rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
                            }
                        } else {//主键缓存中找到了就执行循环的下一轮
                            continue;
                        }
                    }
                }
                if (isFoundPartitionValue) {//分库表
                    Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
                    if(partitionValue == null || partitionValue.size() == 0) {
                        if(tablesRouteMap.get(tableName) == null) {
                            tablesRouteMap.put(tableName, new HashSet<String>());
                        }
                        tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                    } else {
                        for(ColumnRoutePair pair : partitionValue) {
                            if(pair.colValue != null) {
                                Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
                                if(nodeIndex == null) {
                                    String msg = "can't find any valid datanode :" + tableConfig.getName()
                                            + " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
                                    LOGGER.warn(msg);
                                    throw new SQLNonTransientException(msg);
                                }
                                String node = tableConfig.getDataNodes().get(nodeIndex);
                                if(node != null) {
                                    if(tablesRouteMap.get(tableName) == null) {
                                        tablesRouteMap.put(tableName, new HashSet<String>());
                                    }
                                    tablesRouteMap.get(tableName).add(node);
                                }
                            }
                            if(pair.rangeValue != null) {
                                Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm()
                                        .calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
                                for(Integer idx : nodeIndexs) {
                                    String node = tableConfig.getDataNodes().get(idx);
                                    if(node != null) {
                                        if(tablesRouteMap.get(tableName) == null) {
                                            tablesRouteMap.put(tableName, new HashSet<String>());
                                        }
                                        tablesRouteMap.get(tableName).add(node);

                                    }
                                }
                            }
                        }
                    }
                } else if(joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {//childTable  (如果是select 语句的父子表join)之前要找到root table,将childTable移除,只留下root table
                    Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);

                    ColumnRoutePair joinCol = null;

                    Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

                    if (dataNodeSet.isEmpty()) {
                        throw new SQLNonTransientException(
                                "parent key can't find any valid datanode ");
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
                                + Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
                    }
                    if (dataNodeSet.size() > 1) {
                        routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet, sql);
                        rrs.setFinishedRoute(true);
                        return;
                    } else {
                        rrs.setCacheAble(true);
                        routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
                        return;
                    }

                } else {
                    //没找到拆分字段，该表的所有节点都路由
                    if(tablesRouteMap.get(tableName) == null) {
                        tablesRouteMap.put(tableName, new HashSet<String>());
                    }
                    tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
                }
            }
        }
    }

    public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
        boolean isAllGlobal = false;
        for(String table : ctx.getTables()) {
            TableConfig tableConfig = schema.getTables().get(table);
            if(tableConfig!=null && tableConfig.isGlobalTable()) {
                isAllGlobal = true;
            } else {
                return false;
            }
        }
        return isAllGlobal;
    }

    /**
     *
     * @param schema
     * @param ctx
     * @param tc
     * @return true表示校验通过，false表示检验不通过
     */
    public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, TableConfig tc) {
        if(!tc.isRuleRequired()) {
            return true;
        }
        boolean hasRequiredValue = false;
        String tableName = tc.getName();
        if(routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
            hasRequiredValue = false;
        } else {
            for(Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

                String colName = condition.getKey();
                //条件字段是拆分字段
                if(colName.equals(tc.getPartitionColumn())) {
                    hasRequiredValue = true;
                    break;
                }
            }
        }
        return hasRequiredValue;
    }


    /**
     *     增加判断支持未配置分片的表走默认的dataNode
     * @param schemaConfig
     * @param tableName
     * @return
     */
    public static boolean isNoSharding(SchemaConfig schemaConfig,String tableName)
    {
        if(schemaConfig.isNoSharding())
        {
            return true;
        }
        if(schemaConfig.getDataNode()!=null&&!schemaConfig.getTables().containsKey(tableName))
        {
            return true;
        }

        return false;
    }

    /**
     * 判断条件是否永真
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysTrue(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if(Boolean.TRUE.equals(o)) {
            return true;
        }
        return false;
    }

    /**
     * 判断条件是否永假的
     * @param expr
     * @return
     */
    public static boolean isConditionAlwaysFalse(SQLExpr expr) {
        Object o = WallVisitorUtils.getValue(expr);
        if(Boolean.FALSE.equals(o)) {
            return true;
        }
        return false;
    }


    public static boolean processERChildTable(final SchemaConfig schema, final String origSQL,
                                              final MySQLFrontConnection sc) throws SQLNonTransientException {
        String tableName = StringUtil.getTableName(origSQL).toUpperCase();
        final TableConfig tc = schema.getTables().get(tableName);

        if (null != tc && tc.isChildTable()) {
            final RouteResultset rrs = new RouteResultset(origSQL, ServerParse.INSERT);
            String joinKey = tc.getJoinKey();
            MySqlInsertStatement insertStmt = (MySqlInsertStatement) (new MySqlStatementParser(origSQL)).parseInsert();
            int joinKeyIndex = getJoinKeyIndex(insertStmt.getColumns(), joinKey);

            if (joinKeyIndex == -1) {
                String inf = "joinKey not provided :" + tc.getJoinKey() + "," + insertStmt;
                LOGGER.warn(inf);
                throw new SQLNonTransientException(inf);
            }
            if (isMultiInsert(insertStmt)) {
                String msg = "ChildTable multi insert not provided";
                LOGGER.warn(msg);
                throw new SQLNonTransientException(msg);
            }

            String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();

            String sql = insertStmt.toString();

            // try to route by ER parent partion key
            //RouteResultset theRrs = RouterUtil.routeByERParentKey(sc,schema,ServerParse.INSERT,sql, rrs, tc, joinKeyVal);

            if (null != null) {
            	boolean processedInsert=false;
                if ( sc!=null && tc.isAutoIncrement()) {
                    String primaryKey = tc.getPrimaryKey();
                    processedInsert=processInsert(sc,schema,ServerParse.INSERT,sql,tc.getName(),primaryKey);
                }
                if(!processedInsert){
                	rrs.setFinishedRoute(true);
                    sc.getSession2().execute(rrs, ServerParse.INSERT);
                }
                return true;
            }

            // route by sql query root parent's datanode
            final String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + joinKeyVal;
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("find root parent's node sql " + findRootTBSql);
            }

            ListenableFuture<String> listenableFuture = MycatServer.getInstance().
                    getListeningExecutorService().submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler();
                    return fetchHandler.execute(schema.getName(), findRootTBSql, tc.getRootParent().getDataNodes());
                }
            });


            Futures.addCallback(listenableFuture, new FutureCallback<String>() {
                @Override
                public void onSuccess(String result) {
                    if (Strings.isNullOrEmpty(result)) {
                        StringBuilder s = new StringBuilder();
                        LOGGER.warn(s.append(sc.getSession2()).append(origSQL).toString() +
                                " err:" + "can't find (root) parent sharding node for sql:" + origSQL);
                        sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "can't find (root) parent sharding node for sql:" + origSQL);
                        return;
                    }

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("found partion node for child table to insert " + result + " sql :" + origSQL);
                    }
                    boolean processedInsert=false;
                    if ( sc!=null && tc.isAutoIncrement()) {
                        try {
                            String primaryKey = tc.getPrimaryKey();
							processedInsert=processInsert(sc,schema,ServerParse.INSERT,origSQL,tc.getName(),primaryKey);
						} catch (SQLNonTransientException e) {
							LOGGER.warn("sequence processInsert error,",e);
		                    sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR , "sequence processInsert error," + e.getMessage());
						}
                    }
                    if(!processedInsert){
                    	RouteResultset executeRrs = RouterUtil.routeToSingleNode(rrs, result, origSQL);
                        sc.getSession2().execute(executeRrs, ServerParse.INSERT);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    StringBuilder s = new StringBuilder();
                    LOGGER.warn(s.append(sc.getSession2()).append(origSQL).toString() +
                            " err:" + t.getMessage());
                    sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, t.getMessage() + " " + s.toString());
                }
            }, MycatServer.getInstance().
                    getListeningExecutorService());
            return true;
        }
        return false;
    }

    /**
     * 寻找joinKey的索引
     *
     * @param columns
     * @param joinKey
     * @return -1表示没找到，>=0表示找到了
     */
    private static int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
        for (int i = 0; i < columns.size(); i++) {
            String col = StringUtil.removeBackquote(columns.get(i).toString()).toUpperCase();
            if (col.equals(joinKey)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
     *
     * @param insertStmt
     * @return
     */
    private static boolean isMultiInsert(MySqlInsertStatement insertStmt) {
        return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1) || insertStmt.getQuery() != null;
    }

}





