package io.mycat.config.loader.zkprocess.entity.schema.datahost;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * <readHost host="" url="" password="" user=""></readHost>
* 源文件名：ReadHost.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "readHost")
public class ReadHost extends WriteHost {

    @XmlAttribute
    protected String weight;

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    @XmlTransient
    @Override
    public List<ReadHost> getReadHost() {
        return super.getReadHost();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReadHost [weight=");
        builder.append(weight);
        builder.append("]");
        return builder.toString();
    }

}
