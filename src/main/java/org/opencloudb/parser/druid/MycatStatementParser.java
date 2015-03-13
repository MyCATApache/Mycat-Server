package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerTop;
import com.alibaba.druid.sql.parser.Lexer;
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
        exprParser= new MycatExprParser(sql);
    }

    public MycatStatementParser(Lexer lexer)
    {
        super(lexer);
        exprParser= new MycatExprParser(lexer);
    }

    @Override
    public SQLSelectStatement parseSelect()
    {
        MycatSelectParser selectParser = new MycatSelectParser(this.exprParser);
        return new SQLSelectStatement(selectParser.select(), JdbcConstants.MYSQL);
    }

    public SQLSelectParser createSQLSelectParser() {
        return new MycatSelectParser(this.exprParser);
    }

    @Override
    public void accept(Token token)
    {
        if (lexer.token() == Token.TOP) {
            lexer.skipToEOF();
        }
        super.accept(token);
    }




}
