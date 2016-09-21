package io.mycat.config.loader.zkprocess.zookeeper.process;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;

/**
 * 进行zk获取数据类信息
* 源文件名：AbstractLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZkMultLoader {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkMultLoader.class);

    /**
     * zk连接信息
    * @字段说明 curator
    */
    private CuratorFramework curator;

    /**
     * 进行数据转换操作
    * @字段说明 gson
    */
    private Gson gson = new Gson();

    /**
     * 得到树形节点信息
    * 方法描述
    * @param path
    * @param zkDirectory
    * @throws Exception
    * @创建日期 2016年9月15日
    */
    public void getTreeDirectory(String path, String name, DiretoryInf zkDirectory) throws Exception {

        boolean check = this.checkPathExists(path);

        // 如果文件存在，则继续遍历
        if (check) {
            // 首先获取当前节点的数据，然后再递归
            String currDate = this.getDataToString(path);

            List<String> childPathList = this.getChildNames(path);

            // 如果存在子目录信息，则进行
            if (null != childPathList && !childPathList.isEmpty()) {
                DiretoryInf directory = new ZkDirectoryImpl(name, currDate);

                // 添加目录节点信息
                zkDirectory.add(directory);

                for (String childPath : childPathList) {
                    this.getTreeDirectory(path + ZookeeperPath.ZK_SEPARATOR.getKey() + childPath, childPath, directory);
                }
            }
            // 添加当前的数据节点信息
            else {
                zkDirectory.add(new ZkDataImpl(name, currDate));
            }
        }
    }

    /**
     * 检查文件是否存在
    * 方法描述
    * @param path
    * @return
    * @创建日期 2016年9月21日
    */
    protected boolean checkPathExists(String path) {
        try {
            Stat state = this.curator.checkExists().forPath(path);

            if (null != state) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * get data from zookeeper and convert to string with check not null.
     */
    protected String getDataToString(String path) throws Exception {
        byte[] raw = curator.getData().forPath(path);

        checkNotNull(raw, "data of " + path + " must be not null!");
        return byteToString(raw);
    }

    /**
     * get child node name list based on path from zookeeper.
     * @throws Exception 
     */
    protected List<String> getChildNames(String path) throws Exception {
        return curator.getChildren().forPath(path);
    }

    protected void checkAndwriteString(String parentPath, String currpath, String value) throws Exception {
        checkNotNull(parentPath, "data of path" + parentPath + " must be not null!");
        checkNotNull(currpath, "data of path" + currpath + " must be not null!");
        checkNotNull(value, "data of value:" + value + " must be not null!");

        String nodePath = ZKPaths.makePath(parentPath, currpath);

        Stat stat = curator.checkExists().forPath(nodePath);

        if (null == stat) {
            this.createPath(nodePath);
        }

        LOGGER.debug("ZkMultLoader write file :" + nodePath + ", value :" + value);

        curator.setData().inBackground().forPath(nodePath, value.getBytes());

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
    public boolean createPath(String path) {

        // 得到当前的目录信息
        LOGGER.trace("createPath child path is {}", path);

        boolean result = true;
        try {
            // 进行目录的创建操作
            ZKPaths.mkdirs(curator.getZookeeperClient().getZooKeeper(), path);
        } catch (Exception e) {
            LOGGER.error(" createPath error", e);
            result = false;
        }

        return result;
    }

    protected void writeZkString(String path, String value) throws Exception {
        checkNotNull(path, "data of path" + path + " must be not null!");
        checkNotNull(value, "data of value:" + value + " must be not null!");

        curator.setData().forPath(path, value.getBytes());
    }

    /**
     * raw byte data to string
     */
    protected String byteToString(byte[] raw) {
        // return empty json {}.
        if (raw.length == 0) {
            return "{}";
        }
        return new String(raw, StandardCharsets.UTF_8);
    }

    /**
     * 通过名称数据节点信息
    * 方法描述
    * @param zkDirectory
    * @param name
    * @return
    * @创建日期 2016年9月16日
    */
    protected DataInf getZkData(DiretoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof ZkDataImpl) {
                    ZkDataImpl zkDirectoryValue = (ZkDataImpl) directObj;

                    if (name.equals(zkDirectoryValue.getName())) {

                        return zkDirectoryValue;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 通过名称获得目录节点信息
     * 方法描述
     * @param zkDirectory
     * @param name
     * @return
     * @创建日期 2016年9月16日
     */
    protected DiretoryInf getZkDirectory(DiretoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof DiretoryInf) {
                    DiretoryInf zkDirectoryValue = (DiretoryInf) directObj;

                    if (name.equals(zkDirectoryValue.getDataName())) {

                        return zkDirectoryValue;
                    }
                }
            }
        }
        return null;
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }

    public Gson getGson() {
        return gson;
    }

    public void setGson(Gson gson) {
        this.gson = gson;
    }

}
