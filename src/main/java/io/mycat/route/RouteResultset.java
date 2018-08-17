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
package io.mycat.route;

import com.alibaba.druid.sql.ast.SQLStatement;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.parser.util.PageSQLUtil;
import io.mycat.sqlengine.mpp.HavingCols;
import io.mycat.util.FormatUtil;

import java.io.Serializable;
import java.util.*;

/**
 * 路由结果集
 *
 * @author mycat
 */
public final class RouteResultset implements Serializable {
    /**
     * 原始语句
     */
    private String statement;
    /**
     * sql类型
     */
    private final int sqlType;
    /**
     * 路由结果节点
     */
    private RouteResultsetNode[] nodes;
    /**
     * 子表
     */
    private Set<String> subTables;
    /**
     * sql语句对象
     */
    private SQLStatement sqlStatement;

    /**
     * limit开始位置
     */
    private int limitStart;
    /**
     * 是否缓存标志
     */
    private boolean cacheAble;
    /**
     * 主键
     * 用于存储缓存 表ID->datanodes
     * 格式是 table.primaryKey
     *
     * used to store table's ID->datanodes cache
     * format is table.primaryKey
     */
    private String primaryKey;
    /**
     * limit的大小
     */
    private int limitSize;
    /**
     * SQL合并
     */
    private SQLMerge sqlMerge;

    /**
     * 处理call关键字
     */
    private boolean callStatement = false;

    /**
     * 是否为全局表，只有在insert、update、delete、ddl里会判断并修改。默认不是全局表，用于修正全局表修改数据的反馈。
     */
    private boolean globalTableFlag = false;

    /**
     * 是否完成了路由
     */
    private boolean isFinishedRoute = false;

    /**
     * 是否自动提交，此属性主要用于记录ServerConnection上的autocommit状态
     */
    private boolean autocommit = true;

    /**
     * 是否加载数据
     */
    private boolean isLoadData=false;

    /**
     * 是否可以在从库运行,此属性主要供RouteResultsetNode获取
     */
    private Boolean canRunInReadDB;

    /**
     * 默认null表示不施加影响
     * 强制走 master，可以通过 RouteResultset的属性canRunInReadDB=false
     * 传给 RouteResultsetNode 来实现，但是 强制走 slave需要增加一个属性来实现:
     */
    private Boolean runOnSlave = null;

    /**
     * key=dataNode    value=slot
     */
    private Map<String,Integer>   dataNodeSlotMap=new HashMap<>();

    /**
     * 是否是 selectForUpdate
     */
    private boolean selectForUpdate;

    private List<String> tables;

    private Procedure procedure;

    public boolean isSelectForUpdate() {
        return selectForUpdate;
    }

    public void setSelectForUpdate(boolean selectForUpdate) {
        this.selectForUpdate = selectForUpdate;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public Map<String, Integer> getDataNodeSlotMap() {
        return dataNodeSlotMap;
    }

    public void setDataNodeSlotMap(Map<String, Integer> dataNodeSlotMap) {
        this.dataNodeSlotMap = dataNodeSlotMap;
    }

    public Boolean getRunOnSlave() {
		return runOnSlave;
	}
    public String getRunOnSlaveDebugInfo() {
        return runOnSlave == null?"default":Boolean.toString(runOnSlave);
    }
	public void setRunOnSlave(Boolean runOnSlave) {
		this.runOnSlave = runOnSlave;
	}

    public Procedure getProcedure() {
        return procedure;
    }

    public void setProcedure(Procedure procedure) {
        this.procedure = procedure;
    }

    public boolean isLoadData() {
        return isLoadData;
    }

    public void setLoadData(boolean isLoadData) {
        this.isLoadData = isLoadData;
    }

    public boolean isFinishedRoute() {
        return isFinishedRoute;
    }

    public void setFinishedRoute(boolean isFinishedRoute) {
        this.isFinishedRoute = isFinishedRoute;
    }

    public boolean isGlobalTable() {
        return globalTableFlag;
    }

    public void setGlobalTable(boolean globalTableFlag) {
        this.globalTableFlag = globalTableFlag;
    }

    public RouteResultset(String stmt, int sqlType) {
        this.statement = stmt;
        this.limitSize = -1;
        this.sqlType = sqlType;
    }

    /**
     * 重设节点 还原sql语句
     */
    public void resetNodes() {
        if (nodes != null) {
            for (RouteResultsetNode node : nodes) {
                node.resetStatement();
            }
        }
    }

    /**
     * 复制limit参数到节点
     */
    public void copyLimitToNodes() {
        if(nodes!=null) {
            for (RouteResultsetNode node : nodes) {
                if(node.getLimitSize()==-1&&node.getLimitStart()==0) {
                    node.setLimitStart(limitStart);
                    node.setLimitSize(limitSize);
                }
            }
        }
    }

    public SQLMerge getSqlMerge() {
        return sqlMerge;
    }

    public boolean isCacheAble() {
        return cacheAble;
    }

    /**
     * 设置是否要缓存
     * @param cacheAble
     */
    public void setCacheAble(boolean cacheAble) {
        this.cacheAble = cacheAble;
    }

    /**
     * 是否需要合并
     * @return
     */
    public boolean needMerge() {
        return limitSize > 0 || sqlMerge != null;
    }

    public int getSqlType() {
        return sqlType;
    }

    /**
     * 是否有sql count列
     * @return
     */
    public boolean isHasAggrColumn() {
        return (sqlMerge != null) && sqlMerge.isHasAggrColumn();
    }

    public int getLimitStart() {
        return limitStart;
    }

    public String[] getGroupByCols() {
        return (sqlMerge != null) ? sqlMerge.getGroupByCols() : null;
    }

    private SQLMerge createSQLMergeIfNull() {
        if (sqlMerge == null) {
            sqlMerge = new SQLMerge();
        }
        return sqlMerge;
    }

    /**
     * 获取合并列
     * @return
     */
    public Map<String, Integer> getMergeCols() {
        return (sqlMerge != null) ? sqlMerge.getMergeCols() : null;
    }

    public void setLimitStart(int limitStart) {
        this.limitStart = limitStart;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKeyToCache() {
        return primaryKey != null;
    }

    public void setPrimaryKey(String primaryKey) {
        if (!primaryKey.contains(".")) {
            throw new java.lang.IllegalArgumentException(
                    "must be table.primarykey fomat :" + primaryKey);
        }
        this.primaryKey = primaryKey;
    }

    /**
     * 返回主键项，第一个是表名，第二个是主键
     * return primary key items ,first is table name ,seconds is primary key
     * @return
     */
    public String[] getPrimaryKeyItems() {
        return primaryKey.split("\\.");
    }

    public void setOrderByCols(LinkedHashMap<String, Integer> orderByCols) {
        if (orderByCols != null && !orderByCols.isEmpty()) {
            createSQLMergeIfNull().setOrderByCols(orderByCols);
        }
    }

    public void setHasAggrColumn(boolean hasAggrColumn) {
        if (hasAggrColumn) {
            createSQLMergeIfNull().setHasAggrColumn(true);
        }
    }

    public void setGroupByCols(String[] groupByCols) {
        if (groupByCols != null && groupByCols.length > 0) {
            createSQLMergeIfNull().setGroupByCols(groupByCols);
        }
    }

    public void setMergeCols(Map<String, Integer> mergeCols) {
        if (mergeCols != null && !mergeCols.isEmpty()) {
            createSQLMergeIfNull().setMergeCols(mergeCols);
        }
    }

    public LinkedHashMap<String, Integer> getOrderByCols() {
        return (sqlMerge != null) ? sqlMerge.getOrderByCols() : null;
    }

    public String getStatement() {
        return statement;
    }

    public RouteResultsetNode[] getNodes() {
        return nodes;
    }

    public void setNodes(RouteResultsetNode[] nodes) {
        if(nodes!=null) {
            int nodeSize=nodes.length;
            for (RouteResultsetNode node : nodes) {
                node.setTotalNodeSize(nodeSize);
            }
        }
        this.nodes = nodes;
    }

    /**
     * @return -1 if no limit
     */
    public int getLimitSize() {
        return limitSize;
    }

    public void setLimitSize(int limitSize) {
        this.limitSize = limitSize;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public boolean isCallStatement() {
        return callStatement;
    }

    public void setCallStatement(boolean callStatement) {
        this.callStatement = callStatement;
        if(nodes!=null) {
            for (RouteResultsetNode node : nodes) {
                node.setCallStatement(callStatement);
            }
        }
    }

    /**
     * 添加limit后修改节点SQL
     * @param schemaConfig
     * @param sourceDbType
     * @param sql
     * @param offset
     * @param count
     * @param isNeedConvert
     */
    public void changeNodeSqlAfterAddLimit(SchemaConfig schemaConfig, String sourceDbType, String sql, int offset, int count, boolean isNeedConvert) {
        if (nodes != null) {
            Map<String, String> dataNodeDbTypeMap = schemaConfig.getDataNodeDbTypeMap();
            Map<String, String> sqlMapCache = new HashMap<>();
            for (RouteResultsetNode node : nodes) {
                String dbType = dataNodeDbTypeMap.get(node.getName());
                if (dbType.equalsIgnoreCase("mysql")) { // mysql数据库
                    node.setStatement(sql);   //mysql之前已经加好limit
                } else if (sqlMapCache.containsKey(dbType)) {
                    node.setStatement(sqlMapCache.get(dbType));
                } else if(isNeedConvert) { //需要覆盖
                    String nativeSql = PageSQLUtil.convertLimitToNativePageSql(dbType, sql, offset, count); // 将limit转成本地支持的数据库方言的分页sql
                    sqlMapCache.put(dbType, nativeSql);
                    node.setStatement(nativeSql);
                }  else {
                    node.setStatement(sql);
                }
                node.setLimitStart(offset);
                node.setLimitSize(count);
            }
        }
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public Boolean getCanRunInReadDB() {
        return canRunInReadDB;
    }

    public void setCanRunInReadDB(Boolean canRunInReadDB) {
        this.canRunInReadDB = canRunInReadDB;
    }

    public HavingCols getHavingCols() {
        return (sqlMerge != null) ? sqlMerge.getHavingCols() : null;
    }

    public void setSubTables(Set<String> subTables) {
        this.subTables = subTables;
    }

    public void setHavings(HavingCols havings) {
        if (havings != null) {
            createSQLMergeIfNull().setHavingCols(havings);
        }
    }

    // Added by winbill, 20160314, for having clause, Begin ==>
    public void setHavingColsName(Object[] names) {
        if (names != null && names.length > 0) {
            createSQLMergeIfNull().setHavingColsName(names);
        }
    }
    // Added by winbill, 20160314, for having clause, End  <==

    public SQLStatement getSqlStatement() {
        return this.sqlStatement;
    }

    public void setSqlStatement(SQLStatement sqlStatement) {
        this.sqlStatement = sqlStatement;
    }

    public Set<String> getSubTables() {
        return this.subTables;
    }

    /**
     * 路由结果集中是否有不同表 -- 子表
     * @return
     */
    public boolean isDistTable(){
        if(this.getSubTables()!=null && !this.getSubTables().isEmpty() ){
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(statement).append(", route={");
        if (nodes != null) {
            for (int i = 0; i < nodes.length; ++i) {
                s.append("\n ").append(FormatUtil.format(i + 1, 3));
                s.append(" -> ").append(nodes[i]);
            }
        }
        s.append("\n}");
        return s.toString();
    }

}
