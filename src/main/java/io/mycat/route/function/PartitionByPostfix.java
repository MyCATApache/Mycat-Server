package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import org.apache.commons.lang.StringUtils;

/**
 * 根据后缀分表
 * <p>
 * COPYRIGHT © 2001 - 2016 VOYAGE ONE GROUP INC. ALL RIGHTS RESERVED.
 *
 * @author vantis 2017/10/9
 * @version 1.0.0
 */
public class PartitionByPostfix extends AbstractPartitionAlgorithm implements RuleAlgorithm {
    private Integer firstValue;
    private String prefix;
    private String postfix;

    @Override
    public void init() {
        if (null == firstValue) firstValue = 0;
        if (null == prefix) prefix = "";
        if (null == postfix) postfix = "";
    }

    @Override
    public Integer calculate(String columnValue) {
        // 1. 按照 prefix 和 postfix 解析真正的序号
        String finalValue = columnValue;
        if (!StringUtils.isBlank(prefix) && columnValue.startsWith(prefix))
            finalValue = finalValue.substring(finalValue.indexOf(prefix) + 1, finalValue.length());
        if (!StringUtils.isBlank(prefix) && columnValue.endsWith(postfix))
            finalValue = finalValue.substring(0, finalValue.lastIndexOf(postfix));
        return Integer.parseInt(finalValue) - firstValue;
    }

    public Integer getFirst() {
        return firstValue;
    }

    public void setFirst(Integer first) {
        this.firstValue = first;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public Integer getFirstValue() {
        return firstValue;
    }

    public void setFirstValue(Integer firstValue) {
        this.firstValue = firstValue;
    }

    public String getPostfix() {
        return postfix;
    }

    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }
}
