package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * 根据后缀分表
 * <p>
 * COPYRIGHT © 2001 - 2016 VOYAGE ONE GROUP INC. ALL RIGHTS RESERVED.
 *
 * @author vantis 2017/10/9
 * @version 1.0.0
 */
public class PartitionByPostfix extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    @Override
    public Integer calculate(String columnValue) {
        return Integer.parseInt(columnValue) - 1;
    }
}
