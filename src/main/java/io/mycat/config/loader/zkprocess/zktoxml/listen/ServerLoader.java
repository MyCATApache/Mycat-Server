package io.mycat.config.loader.zkprocess.zktoxml.listen;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.entry.Server;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zktoxml.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.zktoxml.comm.notiflyService;
import io.mycat.config.loader.zkprocess.zktoxml.zkProcess.DataInf;
import io.mycat.config.loader.zkprocess.zktoxml.zkProcess.DiretoryInf;
import io.mycat.config.loader.zkprocess.zktoxml.zkProcess.zkdirectry.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zktoxml.zkProcess.zkdirectry.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zktoxml.zkProcess.zkdirectry.ZkDirectoryLoader;

/**
 * 进行server的文件从zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ServerLoader extends ZkDirectoryLoader implements notiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * xml转换对象信息
    * @字段说明 xmlParse
    */
    private XmlProcessBase xmlParse;

    public ServerLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator, XmlProcessBase xmlParse) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        this.xmlParse = xmlParse;

        // 注册转换对象信息
        this.xmlParse.addParseClass(Server.class);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,将集群schema目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf schemaDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey(), schemaDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) schemaDirectory.getSubordinateInfo().get(0);

        String dataPath = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();

        this.getCurator().getZookeeperClient().getZooKeeper().getData(dataPath, new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getState());
                System.out.println("当前路径发生了更新" + event.getPath());
            }

        }, null);

        System.out.println("当前监视的子路径:" + dataPath);

        dataPath = "/mycat/mycat-cluster-1/schema/schema/schema/table";

        // 当前路径的通知
        this.getCurator().getData().usingWatcher(new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("通知信息:" + event);
                System.out.println("收到通知:" + event.getPath());
            }

        }).inBackground().forPath(dataPath);

        String childPath = "/mycat/mycat-cluster-1/schema/schema/schema";

        // 子节点修改的通知
        this.getCurator().getChildren().usingWatcher(new Watcher() {

            @Override
            public void process(WatchedEvent event) {
                System.out.println("子节点通知信息:" + event);
                System.out.println("子节点收到通知:" + event.getPath());
            }

        }).inBackground().forPath(childPath);

        return true;

    }

    /**
     * 通过名称获取路径对象信息
    * 方法描述
    * @param zkDirectory
    * @param name
    * @return
    * @创建日期 2016年9月16日
    */
    private DiretoryInf getZkDirectory(DiretoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof ZkDirectoryImpl) {
                    ZkDirectoryImpl zkDirectoryValue = (ZkDirectoryImpl) directObj;

                    if (name.equals(zkDirectoryValue.getName())) {

                        return zkDirectoryValue;
                    }

                }
            }
        }
        return null;
    }

    /**
     * 带子级的信息转换为json的字符信息，可以为schema以及dataHost转换使用
    * 方法描述
    * @param schemaDirectory
    * @param tojson
    * @创建日期 2016年9月15日
    */
    private void toJsonStr(DiretoryInf schemaDirectory, StringBuilder tojson) {
        List<Object> dirctoryList = schemaDirectory.getSubordinateInfo();

        for (Object object : dirctoryList) {
            // 如果当前为目录节点
            if (object instanceof ZkDirectoryImpl) {
                ZkDirectoryImpl directory = (ZkDirectoryImpl) object;
                String value = directory.getValue();
                tojson.append(value.replaceAll("\\}", ""));
                tojson.append(",");
                this.toJsonStr(directory, tojson);
                tojson.append("}");
            }
            // 如果当前为数据节点
            else if (object instanceof DataInf) {
                ZkDataImpl data = (ZkDataImpl) object;
                tojson.append(data.getName());
                tojson.append(":");
                tojson.append("[");
                tojson.append(data.getValue());
                tojson.append("]");
            }
        }
    }

    /**
     * 将schema的信息转换为json的字符信息
     * 方法描述
     * @param dataNodeZk
     * @param tojson
     * @创建日期 2016年9月15日
     */
    private void toDataNodeJsonStr(DiretoryInf dataNodeZk, StringBuilder tojson) {
        List<Object> dirctoryList = dataNodeZk.getSubordinateInfo();
        tojson.append("[");
        for (int i = 0; i < dirctoryList.size(); i++) {
            Object object = dirctoryList.get(i);
            // 如果当前为数据节点
            if (object instanceof DataInf) {
                ZkDataImpl data = (ZkDataImpl) object;
                tojson.append(data.getValue());
                if (i != dirctoryList.size() - 1) {
                    tojson.append(",");
                }
            }
        }
        tojson.append("]");
    }

}
