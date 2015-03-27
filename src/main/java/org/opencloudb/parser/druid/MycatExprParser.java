package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.Lexer;

/**
 * Created by nange on 2015/3/13.
 */
public class MycatExprParser extends MySqlExprParser
{
    public static String[] max_agg_functions = {"AVG", "COUNT", "GROUP_CONCAT", "MAX", "MIN", "STDDEV", "SUM", "ROW_NUMBER"};

    public MycatExprParser(Lexer lexer)
    {
        super(lexer);
        super.aggregateFunctions = max_agg_functions;
    }

    public MycatExprParser(String sql)
    {
        super(new MycatLexer(sql));
        lexer.nextToken();
        super.aggregateFunctions = max_agg_functions;
    }
}
