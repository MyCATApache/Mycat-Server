package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
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

    @Override
    protected SQLSelectItem parseSelectItem()
    {
        parseTop();
        boolean connectByRoot = false;
        SQLExpr expr;
        if (this.lexer.token() == Token.IDENTIFIER) {
            if (this.identifierEquals("CONNECT_BY_ROOT")) {
                connectByRoot = true;
                this.lexer.nextToken();
            }

            expr = new SQLIdentifierExpr(this.lexer.stringVal());
            this.lexer.nextTokenComma();
            if (this.identifierEquals("DIV")) {
                String div1 = ((SQLIdentifierExpr) expr).getSimpleName();
                this.lexer.nextToken();
                Number div2 = this.lexer.integerValue();
                expr = new SQLIdentifierExpr(div1 + " DIV " + div2);
                SQLExpr expr1 = this.exprParser.primaryRest(expr);
                expr = this.exprParser.exprRest(expr1);
                this.lexer.nextTokenComma();
            } else if (this.lexer.token() != Token.COMMA) {
                SQLExpr expr1 = this.exprParser.primaryRest(expr);
                expr = this.exprParser.exprRest(expr1);
            }
        } else {
            expr = this.expr();
        }

        String alias = this.as();
        return new SQLSelectItem(expr, alias, connectByRoot);
    }


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
