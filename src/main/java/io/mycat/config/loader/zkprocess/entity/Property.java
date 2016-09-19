package io.mycat.config.loader.zkprocess.entity;

import java.util.Objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.XmlValue;

/**
 * 键值对信息
* 源文件名：Property.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月16日
* 修改作者：liujun
* 修改日期：2016年9月16日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Property")
public class Property implements Named {

    @XmlValue
    protected String value;
    @XmlAttribute(name = "name")
    protected String name;

    public String getValue() {
        return value;
    }

    public Property setValue(String value) {
        this.value = value;
        return this;
    }

    public String getName() {
        return name;
    }

    public Property setName(String value) {
        this.name = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Property property = (Property) o;
        return value.equals(property.value) && name.equals(property.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, name);
    }

    @Override
    public String toString() {
        return "Property{" + "value='" + value + '\'' + ", name='" + name + '\'' + '}';
    }
}
