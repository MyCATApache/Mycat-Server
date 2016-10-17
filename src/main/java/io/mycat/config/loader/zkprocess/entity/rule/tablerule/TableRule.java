package io.mycat.config.loader.zkprocess.entity.rule.tablerule;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import io.mycat.config.loader.zkprocess.entity.Named;

/**
 * * <tableRule name="rule1">
 * * *<rule>
 * * * *<columns>id</columns>
 * * * *<algorithm>func1</algorithm>
 * * </rule>
 * </tableRule>
* 源文件名：TableRule.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月18日
* 修改作者：liujun
* 修改日期：2016年9月18日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "tableRule")
public class TableRule implements Named {

    @XmlElement(required = true, name = "rule")
    protected Rule rule;
    
    @XmlAttribute(required = true)
    protected String name;

    public Rule getRule() {
        return rule;
    }

    public TableRule setRule(Rule rule) {
        this.rule = rule;
        return this;
    }

    public String getName() {
        return name;
    }

    public TableRule setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableRule [rule=");
        builder.append(rule);
        builder.append(", name=");
        builder.append(name);
        builder.append("]");
        return builder.toString();
    }
    
}
