package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.util.JdbcConstants;

/**
 * Druid PostgreSQL Select 解析器
 */
public class DruidSelectPostgresqlParser extends DruidSelectParser {
    protected String getCurentDbType() {
        return JdbcConstants.POSTGRESQL;
    }
}
