package io.mycat.config.loader.zkprocess.xmltozk.listen;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.util.IOUtils;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.NotiflyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ParseParamEnum;
import io.mycat.config.loader.zkprocess.entity.Property;
import io.mycat.config.loader.zkprocess.entity.Rules;
import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.FunctionJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.TableRuleJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.xml.RuleParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;

/**
 * 进行从rule.xml加载到zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class RulesxmlTozkLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(RulesxmlTozkLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * Rules文件的路径信息
    * @字段说明 SCHEMA_PATH
    */
    private static final String RULE_PATH = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + "rule.xml";

    /**
     * Rules的xml的转换信息
    * @字段说明 parseRulesXMl
    */
    private ParseXmlServiceInf<Rules> parseRulesXMl;

    /**
     * 表的路由信息
    * @字段说明 parseJsonService
    */
    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    /**
     * 表对应的字段信息
    * @字段说明 parseJsonFunctionService
    */
    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    public RulesxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_RULE.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        // 生成xml与类的转换信息
        parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,读取本地的xml文件
        Rules Rules = this.parseRulesXMl.parseXmlToBean(RULE_PATH);
        LOGGER.info("RulesxmlTozkLoader notiflyProcessxml to zk Rules Object  :" + Rules);
        // 将实体信息写入至zk中
        this.xmlTozkRulesJson(currZkPath, Rules);

        LOGGER.info("RulesxmlTozkLoader notiflyProcess xml to zk is success");

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
    private void xmlTozkRulesJson(String basePath, Rules Rules) throws Exception {
        // tablerune节点信息
        String tableRulePath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_RULE_TABLERULE.getKey();
        String tableRuleJson = this.parseJsonTableRuleService.parseBeanToJson(Rules.getTableRule());
        this.checkAndwriteString(basePath, tableRulePath, tableRuleJson);

        // 读取mapFile文件,并加入到function中
        this.readMapFileAddFunction(Rules.getFunction());

        // 方法设置信息
        String functionPath = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_RULE_FUNCTION.getKey();
        String functionJson = this.parseJsonFunctionService.parseBeanToJson(Rules.getFunction());
        this.checkAndwriteString(basePath, functionPath, functionJson);
    }

    /**
     *  读取序列配制文件便利店  
    * 方法描述
    * @param functionList
    * @创建日期 2016年9月18日
    */
    private void readMapFileAddFunction(List<Function> functionList) {

        List<Property> tempData = new ArrayList<>();

        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                // 进行数据遍历
                for (Property property : proList) {
                    // 如果为mapfile，则需要去读取数据信息，并存到json中
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        Property mapFilePro = new Property();
                        mapFilePro.setName(property.getValue());
                        // 加载属性的值信息
                        mapFilePro.setValue(this.readMapFile(property.getValue()));
                        tempData.add(mapFilePro);
                    }
                }
                // 将数据添加的集合中
                proList.addAll(tempData);
                // 清空，以进行下一次的添加
                tempData.clear();
            }
        }
    }

    /**
     * 读取 mapFile文件的信息
    * 方法描述
    * @param name 名称信息
    * @return
    * @创建日期 2016年9月18日
    */
    private String readMapFile(String name) {

        StringBuilder mapFileStr = new StringBuilder();

        String path = ZookeeperPath.ZK_LOCAL_CFG_PATH.getKey() + name;
        // 加载数据
        InputStream input = RulesxmlTozkLoader.class.getResourceAsStream(path);

        checkNotNull(input, "read Map file curr Path :" + path + " is null! must is not null");

        byte[] buffers = new byte[256];

        try {
            int readIndex = -1;

            while ((readIndex = input.read(buffers)) != -1) {
                mapFileStr.append(new String(buffers, 0, readIndex));
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("RulesxmlTozkLoader readMapFile IOException", e);

        } finally {
            IOUtils.close(input);
        }

        return mapFileStr.toString();
    }

}
