package io.mycat.config.loader.zookeeper.create.comm;

import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zookeeper.create.console.FlowCfg;
import io.mycat.config.loader.zookeeper.create.console.SysFlow;

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
     * 获得基本的路径
    * 方法描述
    * @return
    * @创建日期 2016年9月12日
    */
    public String getBasePath() {
        return SysFlow.ZK_SEPARATOR + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey() + SysFlow.ZK_SEPARATOR
                + String.valueOf(this.zkConfig.get(FlowCfg.FLOW_YAML_CFG_CLUSTER.getKey()));
    }

    /**
     * 从配制文件的yaml文件中提取数据
     * 方法描述
     * @param key 提取的key
     * @return
     * @创建日期 2016年9月12日
     */
    public Object getValue(String key) {
        return this.zkConfig.get(key);
    }

    /**
     * 进行多级的数据的获取
    * 方法描述
    * @param keyArray
    * @param index
    * @return
    * @创建日期 2016年9月13日
    */
    @SuppressWarnings("rawtypes")
    private Object getMapValueByArray(String[] keyArray, int index, Map map) {

        Object getTmp = map;

        // 进行数据层层获取，从最开始的map层层递归
        if (null == map) {
            getTmp = zkConfig.get(keyArray[index]);
        } else {
            if (index < keyArray.length) {
                getTmp = map.get(keyArray[index]);
            }
        }

        if (null != getTmp && getTmp instanceof Map && index < keyArray.length) {
            Map keyMap = (Map) getTmp;
            return this.getMapValueByArray(keyArray, index += 1, keyMap);
        }

        return getTmp;
    }

    /**
     * 创建配制信息
     * 方法描述
     * @param configKey 配制的当前路径名称信息
     * @param filterInnerMap  最终的信息是否为map
     * @param configDirectory 配制的目录
     * @param restDirectory 子目录信息
     * @创建日期 2016年9月11日
     */
    public boolean createPath(String basePath, String createPath) {
        // 得到当前的目录信息
        String childPath = ZKPaths.makePath(basePath, createPath);
        LOGGER.trace("createPath child path is {}", childPath);

        boolean result = true;
        try {
            // 进行目录的创建操作
            ZKPaths.mkdirs(framework.getZookeeperClient().getZooKeeper(), childPath);
        } catch (Exception e) {
            LOGGER.error(" createPath error", e);
            result = false;
        }

        return result;
    }

    /**
     * 删除当前目录 
    * 方法描述
    * @param basePath
    * @return
    * @创建日期 2016年9月13日
    */
    public boolean deletePath(String basePath) {
        // 得到当前的目录信息
        LOGGER.trace("deletePath child path is {}", basePath);

        boolean result = true;
        try {
            // 进行目录的删除操作
            ZKPaths.deleteChildren(framework.getZookeeperClient().getZooKeeper(), basePath, true);
        } catch (Exception e) {
            LOGGER.error("deletePath error", e);
            result = false;
        }
        return result;
    }

    /**
     * 删除子目录下的信息
    * 方法描述
    * @param basePath 父母路径
    * @param childPath 当前节点
    * @return
    * @创建日期 2016年9月13日
    */
    public boolean deletePath(String basePath, String childPath) {
        // 得到当前的目录信息
        String currPath = ZKPaths.makePath(basePath, childPath);
        LOGGER.trace("deletePath is {}", childPath);

        boolean result = true;
        try {
            // 进行目录的删除操作
            ZKPaths.deleteChildren(framework.getZookeeperClient().getZooKeeper(), currPath, true);
        } catch (Exception e) {
            LOGGER.error("deletePath error", e);
            result = false;
        }
        return result;
    }

    /**
     * 创建配制信息
    * 方法描述
    * @param configKey 配制的当前路径名称信息
    * @param filterInnerMap  最终的信息是否为map
    * @param configDirectory 配制的目录
    * @param restDirectory 子目录信息
    * @创建日期 2016年9月11日
    */
    public boolean createConfig(String yarmConfigKey, boolean filterInnerMap, String configDirectory,
            String... restDirectory) {
        boolean result = true;

        // 得到当前的目录信息
        String childPath = ZKPaths.makePath("/", configDirectory, restDirectory);
        LOGGER.trace("child path is {}", childPath);

        try {
            // 进行目录的创建操作
            ZKPaths.mkdirs(framework.getZookeeperClient().getZooKeeper(), childPath);

            // 得到配制的map中的信息
            Object mapObject = null;

            // 检查当前的数据是否为单层获取
            if (yarmConfigKey.indexOf(".") == -1) {
                mapObject = zkConfig.get(yarmConfigKey);
            }
            // 如果数据为多层获取,则需要递归
            else {
                String[] spit = yarmConfigKey.split(SysFlow.ZK_GET_SEP);
                mapObject = this.getMapValueByArray(spit, 0, null);
            }

            // 如果不为空
            if (mapObject != null) {
                // recursion sub map
                if (mapObject instanceof Map) {
                    // 生成子节点与数据信息
                    createChildConfig(mapObject, filterInnerMap, childPath);

                    return result;
                }

                framework.setData().forPath(childPath, JSON.toJSONString(mapObject).getBytes());
            }
        } catch (Exception e) {
            LOGGER.error("error", e);
            result = false;
        }

        return result;
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
    private void createChildConfig(Object mapObject, boolean filterInnerMap, String childPath) {
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
    private void processLeafNode(Map<Object, Object> innerMap, boolean filterInnerMap, String childPath) {
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
