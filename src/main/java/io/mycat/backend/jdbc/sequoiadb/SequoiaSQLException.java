package io.mycat.backend.jdbc.sequoiadb;

import java.sql.SQLException;

@SuppressWarnings("serial")
public class SequoiaSQLException extends SQLException
{

	public SequoiaSQLException(String msg)
    {
        super(msg);
    }

    public static class ErrorSQL extends SequoiaSQLException
    {

		ErrorSQL(String sql)
        {
            super(sql);
        }
    }
}
