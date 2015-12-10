package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.JdbcConstants;

/**
 * Created by nange on 2015/3/13.
 */
public class MycatStatementParser extends MySqlStatementParser
{
    private static final String LOW_PRIORITY   = "LOW_PRIORITY";
    private static final String LOCAL          = "LOCAL";
    private static final String IGNORE         = "IGNORE";
    private static final String CHARACTER      = "CHARACTER";
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


    //此处注释掉，以修正后端jdbc方式时，delete语句解析出错的情况
    //
//    public SQLSelectParser createSQLSelectParser()
//    {
//        return new MycatSelectParser(this.selectExprParser);
//    }

    @Override
    protected MySqlLoadDataInFileStatement parseLoadDataInFile()
    {
        acceptIdentifier("DATA");

        LoadDataStatement stmt = new LoadDataStatement();

        if (identifierEquals(LOW_PRIORITY)) {
            stmt.setLowPriority(true);
            lexer.nextToken();
        }

        if (identifierEquals("CONCURRENT")) {
            stmt.setConcurrent(true);
            lexer.nextToken();
        }

        if (identifierEquals(LOCAL)) {
            stmt.setLocal(true);
            lexer.nextToken();
        }

        acceptIdentifier("INFILE");

        SQLLiteralExpr fileName = (SQLLiteralExpr) exprParser.expr();
        stmt.setFileName(fileName);

        if (lexer.token() == Token.REPLACE) {
            stmt.setReplicate(true);
            lexer.nextToken();
        }

        if (identifierEquals(IGNORE)) {
            stmt.setIgnore(true);
            lexer.nextToken();
        }

        accept(Token.INTO);
        accept(Token.TABLE);

        SQLName tableName = exprParser.name();
        stmt.setTableName(tableName);

        if (identifierEquals(CHARACTER)) {
            lexer.nextToken();
            accept(Token.SET);

            if (lexer.token() != Token.LITERAL_CHARS) {
                throw new ParserException("syntax error, illegal charset");
            }

            String charset = lexer.stringVal();
            lexer.nextToken();
            stmt.setCharset(charset);
        }

        if (identifierEquals("FIELDS") || identifierEquals("COLUMNS")) {
            lexer.nextToken();
            if (identifierEquals("TERMINATED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setColumnsTerminatedBy(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }

            if (identifierEquals("OPTIONALLY")) {
                stmt.setColumnsEnclosedOptionally(true);
                lexer.nextToken();
            }

            if (identifierEquals("ENCLOSED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setColumnsEnclosedBy(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }

            if (identifierEquals("ESCAPED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setColumnsEscaped(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }
        }

        if (identifierEquals("LINES")) {
            lexer.nextToken();
            if (identifierEquals("STARTING")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setLinesStartingBy(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }

            if (identifierEquals("TERMINATED")) {
                lexer.nextToken();
                accept(Token.BY);
                stmt.setLinesTerminatedBy(new SQLCharExpr(lexer.stringVal()));
                lexer.nextToken();
            }
        }

        if (identifierEquals(IGNORE)) {
            lexer.nextToken();
            stmt.setIgnoreLinesNumber( this.exprParser.expr());
            acceptIdentifier("LINES");
        }

        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();
            this.exprParser.exprList(stmt.getColumns(), stmt);
            accept(Token.RPAREN);
        }

        if (lexer.token() == Token.SET) {
            lexer.nextToken();
            this.exprParser.exprList(stmt.getSetList(), stmt);
        }

        return stmt;
    }
}
