package io.mycat.config.loader.zkprocess.entity.rule.tablerule;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * *<rule>
 * * *<columns>id</columns>
 * * *<algorithm>func1</algorithm>
 * *</rule>
* 源文件名：Rule.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月18日
* 修改作者：liujun
* 修改日期：2016年9月18日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "rule", propOrder = { "columns", "algorithm" })
public class Rule {

    protected String columns;
    protected String algorithm;

    public String getColumns() {
        return columns;
    }

    public Rule setColumns(String columns) {
        this.columns = columns;
        return this;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public Rule setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Rule [columns=");
        builder.append(columns);
        builder.append(", algorithm=");
        builder.append(algorithm);
        builder.append("]");
        return builder.toString();
    }
    
}
