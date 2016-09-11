package io.mycat.config.loader.zookeeper.create.comm;

import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * zk操作的基本信息
* 源文件名：ZkProcessBase.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月11日
* 修改作者：liujun
* 修改日期：2016年9月11日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZkProcessBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkProcessBase.class);

    /**
     * zk操作
    * @字段说明 framework
    */
    private CuratorFramework framework;

    /**
     * 配制文件信息
    * @字段说明 zkConfig
    */
    private Map<String, Object> zkConfig;

    /**
     * 创建配制信息
    * 方法描述
    * @param configKey 配制的当前路径名称信息
    * @param filterInnerMap  最终的信息是否为map
    * @param configDirectory 配制的目录
    * @param restDirectory 子目录信息
    * @创建日期 2016年9月11日
    */
    protected void createConfig(String configKey, boolean filterInnerMap, String configDirectory,
            String... restDirectory) {
        // 得到当前的目录信息
        String childPath = ZKPaths.makePath("/", configDirectory, restDirectory);
        LOGGER.trace("child path is {}", childPath);

        try {
            // 进行目录的创建操作
            ZKPaths.mkdirs(framework.getZookeeperClient().getZooKeeper(), childPath);

            // 得到配制的map中的信息
            Object mapObject = zkConfig.get(configKey);
            // recursion sub map
            if (mapObject instanceof Map) {
                createChildConfig(mapObject, filterInnerMap, childPath);
                return;
            }

            if (mapObject != null) {
                framework.setData().forPath(childPath, JSON.toJSONString(mapObject).getBytes());
            }
        } catch (Exception e) {
            LOGGER.error("error", e);
        }
    }

    /**
     * 创建子级的配制信息
    * 方法描述
    * @param mapObject 配制的map信息
    * @param filterInnerMap 过滤的map信息
    * @param childPath 子级目录信息
    * @创建日期 2016年9月11日
    */
    @SuppressWarnings("unchecked")
    protected void createChildConfig(Object mapObject, boolean filterInnerMap, String childPath) {
        if (mapObject instanceof Map) {
            Map<Object, Object> innerMap = (Map<Object, Object>) mapObject;
            for (Map.Entry<Object, Object> entry : innerMap.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    // 进行递归的调用，以进行相关信息的创建
                    createChildConfig(entry.getValue(), filterInnerMap,
                            ZKPaths.makePath(childPath, String.valueOf(entry.getKey())));
                } else {
                    LOGGER.trace("sub child path is {}", childPath);
                    processLeafNode(innerMap, filterInnerMap, childPath);
                }
            }
        }
    }

    /**
     * 录入节点的信息
    * 方法描述
    * @param innerMap 最终的实体map
    * @param filterInnerMap  最终否map信息
    * @param childPath 带子目录的信息
    * @创建日期 2016年9月11日
    */
    protected void processLeafNode(Map<Object, Object> innerMap, boolean filterInnerMap, String childPath) {
        try {
            // 检查是否是否存在
            Stat restNodeStat = framework.checkExists().forPath(childPath);
            if (restNodeStat == null) {
                framework.create().creatingParentsIfNeeded().forPath(childPath);
            }

            // 如果是map，则将map为实体
            if (filterInnerMap) {
                Map<Object, Object> filtered = new HashMap<>();
                for (Map.Entry<Object, Object> entry : innerMap.entrySet()) {
                    if (!(entry.getValue() instanceof Map)) {
                        filtered.put(entry.getKey(), entry.getValue());
                    }
                }
                // 将信息录入到zk中
                framework.setData().forPath(childPath, JSON.toJSONString(filtered).getBytes());
            } else {
                framework.setData().forPath(childPath, JSON.toJSONString(innerMap).toString().getBytes());
            }
        } catch (Exception e) {
            LOGGER.error("create node error: {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public CuratorFramework getFramework() {
        return framework;
    }

    public void setFramework(CuratorFramework framework) {
        this.framework = framework;
    }

    public Map<String, Object> getZkConfig() {
        return zkConfig;
    }

    public void setZkConfig(Map<String, Object> zkConfig) {
        this.zkConfig = zkConfig;
    }

}
