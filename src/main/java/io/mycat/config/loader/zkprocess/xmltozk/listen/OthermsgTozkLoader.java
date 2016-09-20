package io.mycat.config.loader.zkprocess.xmltozk.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 其他一些信息加载到zk中
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class OthermsgTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(OthermsgTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    public OthermsgTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 添加line目录，用作集群中节点，在线的基本目录信息
        String line = currZkPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_LINE.getKey();
        ZKPaths.mkdirs(this.getCurator().getZookeeperClient().getZooKeeper(), line);
        LOGGER.info("OthermsgTozkLoader zookeeper mkdir " + line + " success");

        // 将index_charSet写入到zk目录中

        return true;
    }

    /**
     * 读取 mapFile文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private String readSeqFile(String name) {

        StringBuilder mapFileStr = new StringBuilder();

        String path = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + name;
        // 加载数据
        InputStream input = OthermsgTozkLoader.class.getResourceAsStream(path);

        checkNotNull(input, "read SeqFile file curr Path :" + path + " is null! must is not null");

        byte[] buffers = new byte[256];

        try {
            int readIndex = -1;

            while ((readIndex = input.read(buffers)) != -1) {
                mapFileStr.append(new String(buffers, 0, readIndex));
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("EhcachexmlTozkLoader readMapFile IOException", e);
        } finally {
            IOUtils.close(input);
        }

        return mapFileStr.toString();
    }

}
