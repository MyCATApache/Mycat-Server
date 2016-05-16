package io.mycat.config.table.structure;

/**
 * 将表结构持久化
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:09:03 2016/5/11
 */
public abstract class TableStructureProcessor {
    public abstract void saveTableStructure();
    public abstract void loadTableStructure();
}
