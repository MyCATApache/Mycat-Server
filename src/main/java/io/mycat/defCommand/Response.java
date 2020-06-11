package io.mycat.defCommand;

import java.util.List;
import java.util.function.Supplier;

public interface Response {
    public void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator);
    public void sendError(String t);
}