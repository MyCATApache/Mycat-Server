package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;


public class LoadDataStatement extends MySqlLoadDataInFileStatement
{

    public String toString()
    {
        StringBuilder out = new StringBuilder();
        this.accept(new LoadDataOutputVisitor(out));

        return out.toString();
    }
}
