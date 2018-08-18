package io.mycat.config.loader.zkprocess.zktoxml.listen;

import com.google.common.io.Files;
import io.mycat.MycatServer;
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
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.manager.response.ReloadConfig;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 进行rule的文件从zk中加载
* 源文件名：RuleszkToxmlLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class RuleszkToxmlLoader extends ZkMultLoader implements NotiflyService {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleszkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * 写入本地的文件路径
    * @字段说明 WRITEPATH
    */
    private static final String WRITEPATH = "rule.xml";

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

    /**
     * zk的监控路径信息
    * @字段说明 zookeeperListen
    */
    private ZookeeperProcessListen zookeeperListen;

    public RuleszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
            XmlProcessBase xmlParseBase) {

        this.setCurator(curator);

        this.zookeeperListen = zookeeperListen;

        // 获得当前集群的名称
        String RulesPath = zookeeperListen.getBasePath();
        RulesPath = RulesPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_RULE.getKey();

        currZkPath = RulesPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(RulesPath, this);

        // 生成xml与类的转换信息
        parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notiflyProcess() throws Exception {
        // 1,将集群Rules目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf RulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_RULE.getKey(), RulesDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) RulesDirectory.getSubordinateInfo().get(0);
        Rules Rules = this.zktoRulesBean(zkDirectory);

        LOGGER.info("RuleszkToxmlLoader notiflyProcess zk to object  zk Rules Object  :" + Rules);

        // 将mapfile信息写入到文件 中
        writeMapFileAddFunction(Rules.getFunction());

        LOGGER.info("RuleszkToxmlLoader notiflyProcess write mapFile is success ");

        // 数配制信息写入文件
        String path = RuleszkToxmlLoader.class.getClassLoader().getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey())
                .getPath();
        path = new File(path).getPath() + File.separator;
        path = path + WRITEPATH;

        LOGGER.info("RuleszkToxmlLoader notiflyProcess zk to object writePath :" + path);

        this.parseRulesXMl.parseToXmlWrite(Rules, path, "rule");

        LOGGER.info("RuleszkToxmlLoader notiflyProcess zk to object zk Rules      write :" + path + " is success");

        if (MycatServer.getInstance().getProcessors() != null)
            ReloadConfig.reload();

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
    * 方法描述
    * @param zkDirectory
    * @return
    * @创建日期 2016年9月17日
    */
    private Rules zktoRulesBean(DiretoryInf zkDirectory) {
        Rules Rules = new Rules();

        // tablerule信息
        DataInf RulesZkData = this.getZkData(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_RULE_TABLERULE.getKey());
        List<TableRule> tableRuleData = parseJsonTableRuleService.parseJsonToBean(RulesZkData.getDataValue());
        Rules.setTableRule(tableRuleData);

        // tablerule的监控路径信息
        String watchPath = ZookeeperPath.FLOW_ZK_PATH_RULE.getKey();
        watchPath = watchPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_RULE_TABLERULE.getKey();
        this.zookeeperListen.watchPath(currZkPath, watchPath);

        // 得到function信息
        DataInf functionZkData = this.getZkData(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_RULE_FUNCTION.getKey());
        List<Function> functionList = parseJsonFunctionService.parseJsonToBean(functionZkData.getDataValue());
        Rules.setFunction(functionList);

        // function的监控路径信息
        String functionWatchPath = ZookeeperPath.FLOW_ZK_PATH_RULE.getKey();
        functionWatchPath = functionWatchPath + ZookeeperPath.ZK_SEPARATOR.getKey()
                + ZookeeperPath.FLOW_ZK_PATH_RULE_FUNCTION.getKey();
        this.zookeeperListen.watchPath(currZkPath, functionWatchPath);

        return Rules;
    }

    /**
     *  读取序列配制文件便利店  
    * 方法描述
    * @param functionList
    * @创建日期 2016年9月18日
    */
    private void writeMapFileAddFunction(List<Function> functionList) {

        List<Property> tempData = new ArrayList<>();

        List<Property> writeData = new ArrayList<>();

        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                // 进行数据遍历
                for (Property property : proList) {
                    // 如果为mapfile，则需要去读取数据信息，并存到json中
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        tempData.add(property);
                    }
                }

                // 通过mapfile的名称，找到对应的数据信息
                if (!tempData.isEmpty()) {
                    for (Property property : tempData) {
                        for (Property prozkdownload : proList) {
                            // 根据mapfile的文件名去提取数据
                            if (property.getValue().equals(prozkdownload.getName())) {
                                writeData.add(prozkdownload);
                            }
                        }
                    }
                }

                // 将对应的数据信息写入到磁盘中
                if (!writeData.isEmpty()) {
                    for (Property writeMsg : writeData) {
                        this.writeMapFile(writeMsg.getName(), writeMsg.getValue());
                    }
                }

                // 将数据添加的集合中
                proList.removeAll(writeData);

                // 清空，以进行下一次的添加
                tempData.clear();
                writeData.clear();
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
    private void writeMapFile(String name, String value) {

        // 加载数据
        String path = RuleszkToxmlLoader.class.getClassLoader().getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey())
                .getPath();

        checkNotNull(path, "write Map file curr Path :" + path + " is null! must is not null");
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
