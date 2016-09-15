package io.mycat.config.loader.zktoxml.zkProcess.zkdirectry;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;

import com.google.gson.Gson;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zktoxml.zkProcess.DiretoryInf;

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
public class ZkDirectoryLoader {

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
