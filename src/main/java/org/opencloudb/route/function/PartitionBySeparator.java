package org.opencloudb.route.function;

import org.opencloudb.config.model.rule.RuleAlgorithm;

/**
 * Created by Hash Zhang on 2016/2/14.
 */
public class PartitionBySeparator extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private int separateFactor = 10;

    public void setSeparateFactor(int separateFactor) {
        this.separateFactor = separateFactor;
    }

    @Override
    public Integer calculate(String columnValue) {
        columnValue = columnValue.replace('\'', ' ');
        columnValue = columnValue.trim();
        String prefix = columnValue.substring(0, columnValue.indexOf('-'));
        prefix = prefix.trim();
        return (Integer.parseInt(prefix) / separateFactor);
    }

    public static void main(String args[]) {
        System.out.print(new PartitionBySeparator().calculate("12120' -012"));
    }
}
