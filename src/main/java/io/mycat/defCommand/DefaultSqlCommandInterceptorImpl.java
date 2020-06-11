package io.mycat.defCommand;

import io.mycat.MycatServer;
import io.mycat.server.ServerConnection;

import java.sql.JDBCType;
import java.util.Arrays;


/**
 * 自定义响应
 */
public class DefaultSqlCommandInterceptorImpl implements DefaultSqlCommandInterceptor {


    /**
     * @param c   会话
     * @param sql 当前sql
     * @param response 响应对象
     * @return 是否匹配成功
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
            resultSetBuilder.addColumnInfo("name", JDBCType.VARCHAR);//建议字段类型与值对应
            resultSetBuilder.addColumnInfo("2", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("2", JDBCType.BIGINT);
            resultSetBuilder.addObjectRowPayload(Arrays.asList("amy", 1,2));
            resultSetBuilder.addObjectRowPayload(Arrays.asList("tony", 3,4));
            response.sendResultSet(() -> resultSetBuilder.build());
            return true;//已经匹配并返回结果集
        } catch (Throwable e) {
            response.sendError(e.getLocalizedMessage());
            return false;//是否没有匹配,往后交给路由匹配
        }
    }
}