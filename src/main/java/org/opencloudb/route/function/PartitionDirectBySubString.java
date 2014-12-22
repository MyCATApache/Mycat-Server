package org.opencloudb.route.function;

import org.opencloudb.config.model.rule.RuleAlgorithm;

/**
 * 直接根据字符子串（必须是数字）计算分区号（由应用传递参数，显式指定分区号）。
 * <function name="sub" class="org.opencloudb.route.function.PartitionDirectBySubString">
 * <property name="startIndex">9</property> <!-- zero-based -->
 * <property name="size">2</property>
 * <property name="partitionCount">8</property>
 * <property name="defaultPartition">0</property>
 * </function>
 */
public class PartitionDirectBySubString extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    // 字符子串起始索引（zero-based)
    private int startIndex;
    // 字串长度
    private int size;
    // 分区数量
    private int partitionCount;
    // 默认分区（在分区数量定义时，字串标示的分区编号不在分区数量内时，使用默认分区）
    private int defaultPartition;

    public void setStartIndex(String str) {
        startIndex = Integer.parseInt(str);
    }

    public void setSize(String str) {
        size = Integer.parseInt(str);
    }

    public void setPartitionCount(String partitionCount) {
        this.partitionCount = Integer.parseInt(partitionCount);
    }

    public void setDefaultPartition(String defaultPartition) {
        this.defaultPartition = Integer.parseInt(defaultPartition);
    }

    @Override
    public void init() {

    }

    @Override
    public Integer calculate(String columnValue) {
        String partitionSubString = columnValue.substring(startIndex, startIndex + size);
        int partition = Integer.parseInt(partitionSubString, 10);
        return partitionCount > 0 && partition >= partitionCount
                ? defaultPartition : partition;
    }
}
