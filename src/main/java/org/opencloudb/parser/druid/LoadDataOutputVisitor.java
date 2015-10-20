package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;

/**
 * Created by nange on 2015/10/20.
 */
public class LoadDataOutputVisitor extends MySqlOutputVisitor
{
    public LoadDataOutputVisitor(Appendable appender)
    {
        super(appender);
    }
    @Override
    public boolean visit(MySqlLoadDataInFileStatement x) {
        print("LOAD DATA ");

        if (x.isLowPriority()) {
            print("LOW_PRIORITY ");
        }

        if (x.isConcurrent()) {
            print("CONCURRENT ");
        }

        if (x.isLocal()) {
            print("LOCAL ");
        }

        print("INFILE ");

        x.getFileName().accept(this);

        if (x.isReplicate()) {
            print(" REPLACE ");
        }

        if (x.isIgnore()) {
            print(" IGNORE ");
        }

        print(" INTO TABLE ");
        x.getTableName().accept(this);
        if(x.getCharset()!=null)
        {
            print(" CHARACTER SET ");
            print("'"+x.getCharset()+"'");
        }

        if (x.getColumnsTerminatedBy() != null || x.getColumnsEnclosedBy() != null || x.getColumnsEscaped() != null) {
            print(" COLUMNS");
            if (x.getColumnsTerminatedBy() != null) {
                print(" TERMINATED BY ");
                x.getColumnsTerminatedBy().accept(this);
            }

            if (x.getColumnsEnclosedBy() != null) {
                if (x.isColumnsEnclosedOptionally()) {
                    print(" OPTIONALLY");
                }
                print(" ENCLOSED BY ");
                x.getColumnsEnclosedBy().accept(this);
            }

            if (x.getColumnsEscaped() != null) {
                print(" ESCAPED BY ");
                x.getColumnsEscaped().accept(this);
            }
        }

        if (x.getLinesStartingBy() != null || x.getLinesTerminatedBy() != null) {
            print(" LINES");
            if (x.getLinesStartingBy() != null) {
                print(" STARTING BY ");
                x.getLinesStartingBy().accept(this);
            }

            if (x.getLinesTerminatedBy() != null) {
                print(" TERMINATED BY ");
                x.getLinesTerminatedBy().accept(this);
            }
        }

        if(x.getIgnoreLinesNumber() != null) {
            print(" IGNORE ");
            x.getIgnoreLinesNumber().accept(this);
            print(" LINES");
        }

        if (x.getColumns().size() != 0) {
            print(" (");
            printAndAccept(x.getColumns(), ", ");
            print(")");
        }

        if (x.getSetList().size() != 0) {
            print(" SET ");
            printAndAccept(x.getSetList(), ", ");
        }

        return false;
    }
}
