package io.mycat.config.loader.zkprocess.entity.cache;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 缓存配制信息
* 源文件名：CacheInfo.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月19日
* 修改作者：liujun
* 修改日期：2016年9月19日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
@XmlAccessorType(XmlAccessType.NONE)
@XmlRootElement(name = "defaultCache")
public class CacheInfo {

    /**
     * maxElementsInMemory:在内存中最大的对象数量 
    * @字段说明 maxEntriesLocalHeap
    */
    @XmlAttribute
    private int maxElementsInMemory;

    /**
     *   eternal：设置元素是否永久的，如果为永久，则timeout忽略 
    * @字段说明 maxBytesLocalDisk
    */
    @XmlAttribute
    private boolean eternal;

    /**
     *   overflowToDisk：是否当memory中的数量达到限制后，保存到Disk 
    * @字段说明 updateCheck
    */
    @XmlAttribute
    private boolean overflowToDisk;

    /**
     * diskSpoolBufferSizeMB：这个参数设置DiskStore（磁盘缓存）的缓存区大小。默认是30MB。每个Cache都应该有自己的一个缓冲区。 
    * @字段说明 diskSpoolBufferSizeMB
    */
    @XmlAttribute
    private int diskSpoolBufferSizeMB;

    /**
     *  maxElementsOnDisk：硬盘最大缓存个数。
    * @字段说明 maxElementsOnDisk
    */
    @XmlAttribute
    private int maxElementsOnDisk;

    /**
     *  diskPersistent：是否缓存虚拟机重启期数据 
    * @字段说明 diskPersistent
    */
    @XmlAttribute
    private boolean diskPersistent;

    /**
     * diskExpiryThreadIntervalSeconds：磁盘失效线程运行时间间隔，默认是120秒。 
    * @字段说明 diskExpiryThreadIntervalSeconds
    */
    @XmlAttribute
    private int diskExpiryThreadIntervalSeconds;

    /**
     *  memoryStoreEvictionPolicy：当达到maxElementsInMemory限制时，
     *  Ehcache将会根据指定的策略去清理内存。默认策略是LRU（最近最少使用）。
     *  你可以设置为FIFO（先进先出）或是LFU（较少使用）。    
    * @字段说明 memoryStoreEvictionPolicy
    */
    @XmlAttribute
    private String memoryStoreEvictionPolicy;

    public int getMaxElementsInMemory() {
        return maxElementsInMemory;
    }

    public void setMaxElementsInMemory(int maxElementsInMemory) {
        this.maxElementsInMemory = maxElementsInMemory;
    }

    public boolean isEternal() {
        return eternal;
    }

    public void setEternal(boolean eternal) {
        this.eternal = eternal;
    }

    public boolean isOverflowToDisk() {
        return overflowToDisk;
    }

    public void setOverflowToDisk(boolean overflowToDisk) {
        this.overflowToDisk = overflowToDisk;
    }

    public int getDiskSpoolBufferSizeMB() {
        return diskSpoolBufferSizeMB;
    }

    public void setDiskSpoolBufferSizeMB(int diskSpoolBufferSizeMB) {
        this.diskSpoolBufferSizeMB = diskSpoolBufferSizeMB;
    }

    public int getMaxElementsOnDisk() {
        return maxElementsOnDisk;
    }

    public void setMaxElementsOnDisk(int maxElementsOnDisk) {
        this.maxElementsOnDisk = maxElementsOnDisk;
    }

    public boolean isDiskPersistent() {
        return diskPersistent;
    }

    public void setDiskPersistent(boolean diskPersistent) {
        this.diskPersistent = diskPersistent;
    }

    public int getDiskExpiryThreadIntervalSeconds() {
        return diskExpiryThreadIntervalSeconds;
    }

    public void setDiskExpiryThreadIntervalSeconds(int diskExpiryThreadIntervalSeconds) {
        this.diskExpiryThreadIntervalSeconds = diskExpiryThreadIntervalSeconds;
    }

    public String getMemoryStoreEvictionPolicy() {
        return memoryStoreEvictionPolicy;
    }

    public void setMemoryStoreEvictionPolicy(String memoryStoreEvictionPolicy) {
        this.memoryStoreEvictionPolicy = memoryStoreEvictionPolicy;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CacheInfo [maxElementsInMemory=");
        builder.append(maxElementsInMemory);
        builder.append(", eternal=");
        builder.append(eternal);
        builder.append(", overflowToDisk=");
        builder.append(overflowToDisk);
        builder.append(", diskSpoolBufferSizeMB=");
        builder.append(diskSpoolBufferSizeMB);
        builder.append(", maxElementsOnDisk=");
        builder.append(maxElementsOnDisk);
        builder.append(", diskPersistent=");
        builder.append(diskPersistent);
        builder.append(", diskExpiryThreadIntervalSeconds=");
        builder.append(diskExpiryThreadIntervalSeconds);
        builder.append(", memoryStoreEvictionPolicy=");
        builder.append(memoryStoreEvictionPolicy);
        builder.append("]");
        return builder.toString();
    }

}
