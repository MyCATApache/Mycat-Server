package io.mycat.config.loader.zkprocess.entity.schema.datahost;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

/**
 * <readHost host="" url="" password="" user=""></readHost>
* 源文件名：WriteHost.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "writeHost")
public class WriteHost {

    @XmlAttribute(required = true)
    protected String host;
    @XmlAttribute(required = true)
    protected String url;
    @XmlAttribute(required = true)
    protected String password;
    @XmlAttribute(required = true)
    protected String user;
    @XmlAttribute
    protected Boolean usingDecrypt;

    private List<ReadHost> readHost;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Boolean isUsingDecrypt() {
        return usingDecrypt;
    }

    public void setUsingDecrypt(Boolean usingDecrypt) {
        this.usingDecrypt = usingDecrypt;
    }

    public List<ReadHost> getReadHost() {
        if (this.readHost == null) {
            readHost = new ArrayList<>();
        }
        return readHost;
    }

    public void setReadHost(List<ReadHost> readHost) {
        this.readHost = readHost;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("WriteHost [host=");
        builder.append(host);
        builder.append(", url=");
        builder.append(url);
        builder.append(", password=");
        builder.append(password);
        builder.append(", user=");
        builder.append(user);
        builder.append(", usingDecrypt=");
        builder.append(usingDecrypt);
        builder.append(", readHost=");
        builder.append(readHost);
        builder.append("]");
        return builder.toString();
    }

}
