package org.opencloudb.jdbc.mongodb;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class MongoSQLException extends SQLException
{

	public MongoSQLException(String msg)
    {
        super(msg);
    }

    public static class ErrorSQL extends MongoSQLException
    {

		ErrorSQL(String sql)
        {
            super(sql);
        }
    }
}
