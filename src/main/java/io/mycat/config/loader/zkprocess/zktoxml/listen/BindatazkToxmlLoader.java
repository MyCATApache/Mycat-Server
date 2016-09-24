package io.mycat.config.loader.zkprocess.zktoxml.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 进行从bindata目录从zk加载到本地
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class BindatazkToxmlLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(BindatazkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * zk的监控类信息
    * @字段说明 zookeeperListen
    */
    private ZookeeperProcessListen zookeeperListen;

    public BindatazkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BINDATA.getKey();
        currZkPath = schemaPath;

        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addListen(currZkPath, this);

    }

    @Override
    public boolean notiflyProcess() throws Exception {

        // 通过组合模式进行zk目录树的加载
        DiretoryInf RulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_BINDATA.getKey(), RulesDirectory);

        if (!RulesDirectory.getSubordinateInfo().isEmpty()) {
            // 从当前的下一级开始进行遍历,获得到
            ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) RulesDirectory.getSubordinateInfo().get(0);

            // 首先获邓dnIndex目录信息
            this.reaZkWriteFile(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_BINDATA_DNINDEX.getKey());

            // 写入bindata目录数据
            this.reaZkWriteFile(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_BINDATA_MOVE.getKey());

        }
        LOGGER.info("BindatazkToxmlLoader notiflyProcess   zk ehcache write success ");

        return true;
    }

    /**
     * 读取zk中的信息，将信息写入到文件中
    * 方法描述
    * @param zkDir
    * @param name
    * @创建日期 2016年9月19日
    */
    private void reaZkWriteFile(ZkDirectoryImpl zkDir, String name) {

        if (null != zkDir) {
            // 获得数据的下层级目录到dnindex级
            DiretoryInf zkDirs = this.getZkDirectory(zkDir, name);

            if (zkDir != null && !zkDirs.getSubordinateInfo().isEmpty()) {
                Object zkData = null;

                String watchPath = null;

                for (int i = 0; i < zkDirs.getSubordinateInfo().size(); i++) {
                    zkData = zkDirs.getSubordinateInfo().get(i);
                    if (zkData instanceof ZkDataImpl) {
                        ZkDataImpl dataNode = (ZkDataImpl) zkData;
                        // 将当前节点的数据写入
                        this.writeFileservice(dataNode.getName(), dataNode.getValue());

                        // 获得监控路径信息
                        watchPath = name + ZookeeperPath.ZK_SEPARATOR.getKey() + dataNode.getName();

                        this.zookeeperListen.watchPath(currZkPath, watchPath);
                    }
                }
            }
        }

    }

    /**
     * 写入文件信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private void writeFileservice(String name, String value) {

        // 加载数据
        String path = RuleszkToxmlLoader.class.getClassLoader().getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey())
                .getPath();

        checkNotNull(path, "write  file curr Path :" + path + " is null! must is not null");

        path = path.substring(1) + name;

        ByteArrayInputStream input = null;
        byte[] buffers = new byte[3];
        FileOutputStream output = null;

        try {
            int readIndex = -1;
            input = new ByteArrayInputStream(value.getBytes());
            output = new FileOutputStream(path);

            while ((readIndex = input.read(buffers)) != -1) {
                output.write(buffers, 0, readIndex);
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("RulesxmlTozkLoader readMapFile IOException", e);

        } finally {
            IOUtils.close(output);
            IOUtils.close(input);
        }

    }

}
