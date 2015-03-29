package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.JdbcConstants;

/**
 * Created by nange on 2015/3/13.
 */
public class MycatStatementParser extends MySqlStatementParser
{

    public MycatStatementParser(String sql)
    {
        super(sql);
        selectExprParser = new MycatExprParser(sql);
    }

    public MycatStatementParser(Lexer lexer)
    {
        super(lexer);
        selectExprParser = new MycatExprParser(lexer);
    }

    protected SQLExprParser selectExprParser;
    @Override
    public SQLSelectStatement parseSelect()
    {

        MycatSelectParser selectParser = new MycatSelectParser(this.selectExprParser);
        return new SQLSelectStatement(selectParser.select(), JdbcConstants.MYSQL);
    }

    public SQLSelectParser createSQLSelectParser()
    {
        return new MycatSelectParser(this.selectExprParser);
    }



}
