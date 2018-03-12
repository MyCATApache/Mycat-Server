package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;

/**
 * Created by nange on 2015/3/13.
 */
public class MycatSelectParser extends MySqlSelectParser
{
    public MycatSelectParser(SQLExprParser exprParser)
    {
        super(exprParser);
    }

    public MycatSelectParser(String sql)
    {
        super(sql);
    }


//public SQLSelectQuery query()
//{
//    parseTop();
//    return super.query();
//}

    public void parseTop()
    {
        if (lexer.token() == Token.TOP)
        {
            lexer.nextToken();

            boolean paren = false;
            if (lexer.token() == Token.LPAREN)
            {
                paren = true;
                lexer.nextToken();
            }

            if (paren)
            {
                accept(Token.RPAREN);
            }

            if (lexer.token() == Token.LITERAL_INT)
            {
                lexer.mark();
                lexer.nextToken();
            }
            if (lexer.token() == Token.IDENTIFIER)
            {
                lexer.nextToken();

            }
            if (lexer.token() == Token.EQ||lexer.token() == Token.DOT)
            {
                lexer.nextToken();
            } else  if(lexer.token() != Token.STAR)
            {
                lexer.reset();
            }
            if (lexer.token() == Token.PERCENT)
            {
                lexer.nextToken();
            }


        }


    }
}
