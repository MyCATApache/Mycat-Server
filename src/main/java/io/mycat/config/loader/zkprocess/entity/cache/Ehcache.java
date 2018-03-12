package io.mycat.config.loader.zkprocess.entity.cache;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * ehcache配制信息
* 源文件名：Ehcache.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月19日
* 修改作者：liujun
* 修改日期：2016年9月19日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ehcache")
public class Ehcache {

    /**
     * 
    * @字段说明 maxEntriesLocalHeap
    */
    @XmlAttribute
    private int maxEntriesLocalHeap;

    /**
    * @字段说明 maxBytesLocalDisk
    */
    @XmlAttribute
    private String maxBytesLocalDisk;

    /**
    * @字段说明 updateCheck
    */
    @XmlAttribute
    private boolean updateCheck;

    /**
     * 缓存信息
    * @字段说明 defaultCache
    */
    @XmlElement
    private CacheInfo defaultCache;

    public int getMaxEntriesLocalHeap() {
        return maxEntriesLocalHeap;
    }

    public void setMaxEntriesLocalHeap(int maxEntriesLocalHeap) {
        this.maxEntriesLocalHeap = maxEntriesLocalHeap;
    }

    public String getMaxBytesLocalDisk() {
        return maxBytesLocalDisk;
    }

    public void setMaxBytesLocalDisk(String maxBytesLocalDisk) {
        this.maxBytesLocalDisk = maxBytesLocalDisk;
    }

    public boolean isUpdateCheck() {
        return updateCheck;
    }

    public void setUpdateCheck(boolean updateCheck) {
        this.updateCheck = updateCheck;
    }

    public CacheInfo getDefaultCache() {
        return defaultCache;
    }

    public void setDefaultCache(CacheInfo defaultCache) {
        this.defaultCache = defaultCache;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Ehcache [maxEntriesLocalHeap=");
        builder.append(maxEntriesLocalHeap);
        builder.append(", maxBytesLocalDisk=");
        builder.append(maxBytesLocalDisk);
        builder.append(", updateCheck=");
        builder.append(updateCheck);
        builder.append(", defaultCache=");
        builder.append(defaultCache);
        builder.append("]");
        return builder.toString();
    }

}
