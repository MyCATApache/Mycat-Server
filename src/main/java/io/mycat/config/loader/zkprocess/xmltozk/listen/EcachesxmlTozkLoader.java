package io.mycat.config.loader.zkprocess.xmltozk.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 进行从ecache.xml加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class EcachesxmlTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(EcachesxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * Ehcache文件的路径信息
    * @字段说明 SCHEMA_PATH
    */
    private static final String EHCACHE_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "ehcache.xml";

    /**
     * 缓存文件名称
    * @字段说明 CACHESERVER_NAME
    */
    private static final String CACHESERVER_NAME = "cacheservice.properties";

    /**
     * 缓存的xml文件配制信息
    * @字段说明 EHCACHE_NAME
    */
    private static final String EHCACHE_NAME = "ehcache.xml";

    /**
     * ehcache的xml的转换信息
    * @字段说明 parseEhcacheXMl
    */
    private final ParseXmlServiceInf<Ehcache> parseEcacheXMl;

    /**
     * 表的路由信息
    * @字段说明 parseJsonService
    */
    private ParseJsonServiceInf<Ehcache> parseJsonEhcacheService = new EhcacheJsonParse();

    public EcachesxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_CACHE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,读取本地的xml文件
        Ehcache Ehcache = this.parseEcacheXMl.parseXmlToBean(EHCACHE_PATH);
        LOGGER.info("EhcachexmlTozkLoader notiflyProcessxml to zk Ehcache Object  :" + Ehcache);
        // 将实体信息写入至zk中
        this.xmlTozkEhcacheJson(currZkPath, Ehcache);

        LOGGER.info("EhcachexmlTozkLoader notiflyProcess xml to zk is success");

        return true;
    }

    /**
     * 将xml文件的信息写入到zk中
    * 方法描述
    * @param basePath 基本路径
    * @param schema schema文件的信息
    * @throws Exception 异常信息
    * @创建日期 2016年9月17日
    */
    private void xmlTozkEhcacheJson(String basePath, Ehcache ehcache) throws Exception {
        // ehcache节点信息
        String ehcacheFile = ZookeeperPath.ZK_SEPARATOR.getKey() + EHCACHE_NAME;
        String ehcacheJson = this.parseJsonEhcacheService.parseBeanToJson(ehcache);
        this.checkAndwriteString(basePath, ehcacheFile, ehcacheJson);

        // 读取文件信息
        String cacheServicePath = ZookeeperPath.ZK_SEPARATOR.getKey() + CACHESERVER_NAME;
        String serviceValue = this.readSeqFile(CACHESERVER_NAME);
        this.checkAndwriteString(basePath, cacheServicePath, serviceValue);
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
        InputStream input = EcachesxmlTozkLoader.class.getResourceAsStream(path);

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
