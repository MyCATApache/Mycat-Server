package org.opencloudb.parser.druid.impl;

import com.alibaba.druid.util.JdbcConstants;

public class DruidSelectPostgresqlParser extends DruidSelectParser
{


    protected String getCurentDbType()
    {
        return JdbcConstants.POSTGRESQL;
    }


}
