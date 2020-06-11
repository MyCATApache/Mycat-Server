package io.mycat.defCommand;

import io.mycat.server.ServerConnection;

public interface DefaultSqlCommandInterceptor {
    boolean match(ServerConnection c, String sql,Response response);
    boolean handle(ServerConnection c, String sql,Response response);
}