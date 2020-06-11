package io.mycat.defCommand;

import io.mycat.MycatServer;
import io.mycat.server.ServerConnection;

import java.sql.JDBCType;
import java.util.Arrays;

public class DefaultSqlCommandInterceptorImpl implements DefaultSqlCommandInterceptor {


    /**
     * @param c        会话
     * @param sql
     * @param response
     * @return
     */
    @Override
    public boolean match(ServerConnection c, String sql, Response response) {
        String schema = c.getSchema();//默认库
        if ("select 1,2".equalsIgnoreCase(sql)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean handle(ServerConnection c, String sql, Response response) {
        try {
            MycatServer instance = MycatServer.getInstance();

//        Map<String, SchemaConfig> schemas = instance.getConfig().getSchemas();
//        TableConfig tableConfig = schemas.get("database").getTables().get("tableName");//获取表配置
//        ArrayList<String> dataNodes = tableConfig.getDataNodes();//获取dataNode
//        RuleConfig rule = tableConfig.getRule();//获取分片算法

//        PhysicalDBPool dataHost = MycatServer.getInstance().getConfig().getDataHosts().get("dataHost");
//        JDBCDatasource datasource = (JDBCDatasource)dataHost.genAllDataSources().stream().filter(i -> "name".equalsIgnoreCase(i.getName())).findFirst().get();
//        Connection druidConnection = datasource.getDruidConnection();//jdbc 连接
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            resultSetBuilder.addColumnInfo("1", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("2", JDBCType.VARCHAR);
            resultSetBuilder.addObjectRowPayload(Arrays.asList("1", "2"));
            resultSetBuilder.addObjectRowPayload(Arrays.asList("3", "4"));
            response.sendResultSet(() -> resultSetBuilder.build());
            return true;//已经匹配并返回结果集
        } catch (Throwable e) {
            response.sendError(e.getLocalizedMessage());
            return false;//是否没有匹配,往后交给路由匹配
        }
    }
}