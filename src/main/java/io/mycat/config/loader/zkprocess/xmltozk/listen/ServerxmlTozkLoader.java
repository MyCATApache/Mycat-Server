package io.mycat.config.loader.zkprocess.xmltozk.listen;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.Server;
import io.mycat.config.loader.zkprocess.entity.server.System;
import io.mycat.config.loader.zkprocess.entity.server.user.User;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.SystemJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.json.UserJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.server.xml.ServerParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 进行从server.xml加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ServerxmlTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * server文件的路径信息
    * @字段说明 SCHEMA_PATH
    */
    private static final String SERVER_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "server.xml";

    /**
     * index_to_charset文件的路径信息
     * @字段说明 SCHEMA_PATH
     */
    private static final String INDEX_TOCHARSET_PATH = "index_to_charset.properties";

    /**
     * server的xml的转换信息
    * @字段说明 parseServerXMl
    */
    private ParseXmlServiceInf<Server> parseServerXMl;

    /**
     * system信息
    * @字段说明 parseJsonSchema
    */
    private ParseJsonServiceInf<System> parseJsonSystem = new SystemJsonParse();

    /**
     * system信息
     * @字段说明 parseJsonSchema
     */
    private ParseJsonServiceInf<List<User>> parseJsonUser = new UserJsonParse();

    public ServerxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseServerXMl = new ServerParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,读取本地的xml文件
        Server server = this.parseServerXMl.parseXmlToBean(SERVER_PATH);
        LOGGER.info("ServerxmlTozkLoader notiflyProcessxml to zk server Object  :" + server);
        // 将实体信息写入至zk中
        this.xmlTozkServerJson(currZkPath, server);

        // 2,读取集群中的节点信息
        this.writeClusterNode(currZkPath);

        // 读取properties
        String charSetValue = readProperties(INDEX_TOCHARSET_PATH);
        // 将文件上传
        this.checkAndwriteString(currZkPath, INDEX_TOCHARSET_PATH, charSetValue);

        LOGGER.info("ServerxmlTozkLoader notiflyProcess xml to zk is success");

        return true;
    }

    /**
     * 写入集群节点的信息
    * 方法描述
    * @throws Exception
    * @创建日期 2016年9月17日
    */
    private void writeClusterNode(String basePath) throws Exception {
        // 1，读取集群节点信息
        String[] zkNodes = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTER_NODES)
                .split(ZkParamCfg.ZK_CFG_CLUSTER_NODES_SEPARATE.getKey());

        if (null != zkNodes && zkNodes.length > 0) {
            for (String node : zkNodes) {
                String nodePath = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "server-" + node + ".xml";
                // 将当前的xml文件写入到zk中
                Server serverNode = this.parseServerXMl.parseXmlToBean(nodePath);

                LOGGER.info("ServerxmlTozkLoader writeClusterNode to zk server Object  :" + serverNode);

                // 如果当前不存在此配制文件则不写入
                if (null != serverNode) {
                    // 以集群的节点的名称写入
                    this.xmlTozkClusterNodeJson(basePath, node, serverNode);

                    LOGGER.info("ServerxmlTozkLoader writeClusterNode xml to zk is success");
                }
            }
        }
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param schema schema文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void xmlTozkServerJson(String basePath, Server server) throws Exception {
        // 设置默认的节点信息
        String defaultSystem = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER_DEFAULT.getKey();
        String defaultSystemValue = this.parseJsonSystem.parseBeanToJson(server.getSystem());
        this.checkAndwriteString(basePath, defaultSystem, defaultSystemValue);

        // 设置用户信息
        String userStr = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER_USER.getKey();
        String userValueStr = this.parseJsonUser.parseBeanToJson(server.getUser());
        this.checkAndwriteString(basePath, userStr, userValueStr);
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param schema schema文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void xmlTozkClusterNodeJson(String basePath, String node, Server server) throws Exception {
        // 设置集群中的节点信息
        basePath = basePath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SERVER_CLUSTER.getKey();
        String clusterSystemValue = this.parseJsonSystem.parseBeanToJson(server.getSystem());
        this.checkAndwriteString(basePath, node, clusterSystemValue);
    }

    /**
     * 读取 properties配制文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private String readProperties(String name) {

        String path = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + name;
        // 加载数据
        InputStream input = SequenceTozkLoader.class.getResourceAsStream(path);

        if (null != input) {

            StringBuilder mapFileStr = new StringBuilder();

            byte[] buffers = new byte[256];

            try {
                int readIndex = -1;

                while ((readIndex = input.read(buffers)) != -1) {
                    mapFileStr.append(new String(buffers, 0, readIndex));
                }
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.error("SequenceTozkLoader readMapFile IOException", e);

            } finally {
                IOUtils.close(input);
            }

            return mapFileStr.toString();
        }
        return null;
    }

}
