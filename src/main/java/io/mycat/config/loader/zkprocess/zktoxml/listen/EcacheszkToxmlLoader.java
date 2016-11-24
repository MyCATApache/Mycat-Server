package io.mycat.config.loader.zkprocess.zktoxml.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.cache.Ehcache;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.json.EhcacheJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.cache.xml.EhcacheParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
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
public class EcacheszkToxmlLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(EcacheszkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

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

    /**
     * 监控类信息
    * @字段说明 zookeeperListen
    */
    private ZookeeperProcessListen zookeeperListen;

    public EcacheszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_CACHE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        this.zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseEcacheXMl = new EhcacheParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {

        // 通过组合模式进行zk目录树的加载
        DiretoryInf RulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_CACHE.getKey(), RulesDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) RulesDirectory.getSubordinateInfo().get(0);

        // 进行写入操作
        zktoEhcacheWrite(zkDirectory);

        LOGGER.info("EcacheszkToxmlLoader notiflyProcess   zk ehcache write success ");

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
    * 方法描述
    * @param zkDirectory
    * @return
    * @创建日期 2016年9月17日
    */
    private void zktoEhcacheWrite(ZkDirectoryImpl zkDirectory) {

        // 得到schema对象的目录信息
        DataInf ehcacheZkDirectory = this.getZkData(zkDirectory, EHCACHE_NAME);

        Ehcache ehcache = parseJsonEhcacheService.parseJsonToBean(ehcacheZkDirectory.getDataValue());

        String outputPath = EcacheszkToxmlLoader.class.getClassLoader()
                .getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey()).getPath();
        outputPath = new File(outputPath).getPath() + File.separator;
        outputPath += EHCACHE_NAME;

        parseEcacheXMl.parseToXmlWrite(ehcache, outputPath, null);

        // 设置zk监控的路径信息
        String watchPath = zkDirectory.getName();
        watchPath = watchPath + ZookeeperPath.ZK_SEPARATOR.getKey() + EHCACHE_NAME;
        this.zookeeperListen.watchPath(currZkPath, watchPath);

        // 写入cacheservice.properties的信息
        DataInf cacheserZkDirectory = this.getZkData(zkDirectory, CACHESERVER_NAME);

        if (null != cacheserZkDirectory) {
            ZkDataImpl cacheData = (ZkDataImpl) cacheserZkDirectory;

            // 写入文件cacheservice.properties
            this.writeCacheservice(cacheData.getName(), cacheData.getValue());

            String watchServerPath = zkDirectory.getName();
            watchServerPath = watchPath + ZookeeperPath.ZK_SEPARATOR.getKey() + CACHESERVER_NAME;
            this.zookeeperListen.watchPath(currZkPath, watchServerPath);
        }

    }

    /**
     * 读取 mapFile文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private void writeCacheservice(String name, String value) {

        // 加载数据
        String path = RuleszkToxmlLoader.class.getClassLoader().getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey())
                .getPath();

        checkNotNull(path, "write ecache file curr Path :" + path + " is null! must is not null");
        path = new File(path).getPath() + File.separator;
        path += name;

        // 进行数据写入
        try {
            Files.write(value.getBytes(), new File(path));
        } catch (IOException e1) {
            e1.printStackTrace();
        }

    }

}
