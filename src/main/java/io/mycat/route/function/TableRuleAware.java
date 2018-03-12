package io.mycat.route.function;

/**
 * Created by magicdoom on 2016/9/5.
 *  考虑一类新分片算法     属于有状态算法
 *  比如PartitionByCRC32PreSlot 如果迁移过数据的话，slot映射规则会进行改变
 *  所以必须对应一张表单独一个实例      实现此接口后会根据不同表自动创建新实例
 */
public interface TableRuleAware {
     void setTableName(String tableName);
    void setRuleName(String ruleName);
    String getTableName();
    String getRuleName();
}
