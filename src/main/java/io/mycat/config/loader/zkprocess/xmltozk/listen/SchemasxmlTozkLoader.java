package io.mycat.config.loader.zkprocess.xmltozk.listen;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.entity.Schemas;
import io.mycat.config.loader.zkprocess.entity.schema.datahost.DataHost;
import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataHostJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataNodeJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.SchemaJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 进行从xml加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SchemasxmlTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemasxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * schema文件的路径信息
    * @字段说明 SCHEMA_PATH
    */
    private static final String SCHEMA_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "schema.xml";

    /**
     * schema类与xml转换服务 
    * @字段说明 parseSchemaService
    */
    private ParseXmlServiceInf<Schemas> parseSchemaXmlService;

    /**
     * 进行将schema
    * @字段说明 parseJsonSchema
    */
    private ParseJsonServiceInf<List<Schema>> parseJsonSchema = new SchemaJsonParse();

    /**
     * 进行将dataNode
     * @字段说明 parseJsonSchema
     */
    private ParseJsonServiceInf<List<DataNode>> parseJsonDataNode = new DataNodeJsonParse();

    /**
     * 进行将dataNode
     * @字段说明 parseJsonSchema
     */
    private ParseJsonServiceInf<List<DataHost>> parseJsonDataHost = new DataHostJsonParse();

    public SchemasxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        this.parseSchemaXmlService = new SchemasParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,读取本地的xml文件
        Schemas schema = this.parseSchemaXmlService.parseXmlToBean(SCHEMA_PATH);

        LOGGER.info("SchemasxmlTozkLoader notiflyProcessxml to zk schema Object  :" + schema);

        // 将实体信息写入至zk中
        this.xmlTozkSchemasJson(currZkPath, schema);

        LOGGER.info("SchemasxmlTozkLoader notiflyProcess xml to zk is success");

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
    private void xmlTozkSchemasJson(String basePath, Schemas schema) throws Exception {

        // 设置schema目录的值
        String schemaStr = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey();

        String schemaValueStr = this.parseJsonSchema.parseBeanToJson(schema.getSchema());

        this.checkAndwriteString(basePath, schemaStr, schemaValueStr);
        // 设置datanode
        String dataNodeStr = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey();

        String dataNodeValueStr = this.parseJsonDataNode.parseBeanToJson(schema.getDataNode());

        this.checkAndwriteString(basePath, dataNodeStr, dataNodeValueStr);

        // 设置dataHost
        String dataHostStr = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey();

        String dataHostValueStr = this.parseJsonDataHost.parseBeanToJson(schema.getDataHost());

        this.checkAndwriteString(basePath, dataHostStr, dataHostValueStr);

    }

}
