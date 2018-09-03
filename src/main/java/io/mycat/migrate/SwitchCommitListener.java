package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.mycat.MycatServer;
import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.entity.Rules;
import io.mycat.config.loader.zkprocess.entity.Schemas;
import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import io.mycat.config.loader.zkprocess.entity.schema.datahost.DataHost;
import io.mycat.config.loader.zkprocess.entity.schema.datanode.DataNode;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Schema;
import io.mycat.config.loader.zkprocess.entity.schema.schema.Table;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.FunctionJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.TableRuleJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.xml.RuleParseXmlImpl;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataHostJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.DataNodeJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.json.SchemaJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.schema.xml.SchemasParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.ClusterInfo;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDataImpl;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.manager.response.ReloadConfig;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;
import io.mycat.route.function.TableRuleAware;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by magicdoom on 2016/12/19.
 */
public class SwitchCommitListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchCommitListener.class);

    @Override
    public void childEvent(CuratorFramework curatorFramework,
                           PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:
                checkCommit(event);
                break;
            default:
                break;
        }
    }

    private void checkCommit(PathChildrenCacheEvent event) {
        InterProcessMutex taskLock = null;
        try {

            String path = event.getData().getPath();
            String taskPath = path.substring(0, path.lastIndexOf("/_commit/"));
            String taskID = taskPath.substring(taskPath.lastIndexOf('/') + 1, taskPath.length());
            String lockPath = ZKUtils.getZKBasePath() + "lock/" + taskID + ".lock";
            List<String> sucessDataHost = ZKUtils.getConnection().getChildren().forPath(path.substring(0, path.lastIndexOf('/')));
            String custerName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);
            ClusterInfo clusterInfo = JSON.parseObject(ZKUtils.getConnection().getData().forPath("/mycat/" + custerName), ClusterInfo.class);
            List<String> clusterNodeList = Splitter.on(',').omitEmptyStrings().splitToList(clusterInfo.getClusterNodes());
            //等待所有的dataHost都导出数据完毕。 dataHost的数量== booster的数量
            //判断条件需要进行修改  todo
            if (sucessDataHost.size() == clusterNodeList.size()) {

                List<String> taskDataHost = ZKUtils.getConnection().getChildren().forPath(taskPath);
                List<MigrateTask> allTaskList = MigrateUtils.queryAllTask(taskPath, taskDataHost);
                taskLock = new InterProcessMutex(ZKUtils.getConnection(), lockPath);
                taskLock.acquire(120, TimeUnit.SECONDS);
                TaskNode taskNode = JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath), TaskNode.class);
                if (taskNode.getStatus() == 2) {
                    taskNode.setStatus(3);
                    //开始切换 且个节点已经禁止写入并且无原有写入在执行
                    try {
                        CuratorTransactionFinal transactionFinal = null;
                        check(taskID, allTaskList);
                        SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(taskNode.getSchema());
                        TableConfig tableConfig = schemaConfig.getTables().get(taskNode.getTable().toUpperCase());
                        List<String> newDataNodes = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(taskNode.getAdd());
                        List<String> allNewDataNodes = tableConfig.getDataNodes();
                        allNewDataNodes.addAll(newDataNodes);
                        //先修改rule config
                        InterProcessMutex ruleLock = new InterProcessMutex(ZKUtils.getConnection(), ZKUtils.getZKBasePath() + "lock/rules.lock");
                        ;
                        try {
                            ruleLock.acquire(30, TimeUnit.SECONDS);
                            //transactionFinal = modifyZkRules(transactionFinal, tableConfig.getRule().getFunctionName(), newDataNodes);
                            transactionFinal = modifyTableConfigRules(transactionFinal, taskNode.getSchema(), taskNode.getTable(), newDataNodes);
                        } finally {
                            ruleLock.release();
                        }

                        transactionFinal = modifyRuleData(transactionFinal, allTaskList, tableConfig, allNewDataNodes);
                        transactionFinal.setData().forPath(taskPath, JSON.toJSONBytes(taskNode));

                        clean(taskID, allTaskList);
                        transactionFinal.commit();

                        forceTableRuleToLocal(taskPath, taskNode);
                        pushACKToClean(taskPath);
                    } catch (Exception e) {
                        taskNode.addException(e.getLocalizedMessage());
                        //异常to  Zk
                        ZKUtils.getConnection().setData().forPath(taskPath, JSON.toJSONBytes(taskNode));
                        LOGGER.error("error:", e);
                        return;
                    }
                    //todo   清理规则     顺利拉下ruledata保证一定更新到本地
                } else if (taskNode.getStatus() == 3) {
                    forceTableRuleToLocal(taskPath, taskNode);
                    pushACKToClean(taskPath);
                }

            }


        } catch (Exception e) {
            LOGGER.error("migrate 中 commit 阶段异常");
            LOGGER.error("error:", e);
        } finally {
            if (taskLock != null) {
                try {
                    taskLock.release();
                } catch (Exception ignored) {

                }
            }
        }
    }

    private void forceTableRuleToLocal(String path, TaskNode taskNode) throws Exception {
        Path localPath = Paths.get(this.getClass()
                .getClassLoader()
                .getResource(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey()).toURI());
        // 获得公共的xml转换器对象
        XmlProcessBase xmlProcess = new XmlProcessBase();

        try {
            forceTableToLocal(localPath, xmlProcess);
            forceRulesToLocal(localPath, xmlProcess);
            //保证先有table再有rule
            forceRuleDataToLocal(taskNode);
            ReloadConfig.reload();
            LOGGER.error("migrate 中 reload 配置成功");
        } catch (Exception e) {
            taskNode.addException(e.getLocalizedMessage());
            //异常to  Zk
            ZKUtils.getConnection().setData().forPath(path, JSON.toJSONBytes(taskNode));
            LOGGER.error("migrate 中 强制更新本地文件失败");
            LOGGER.error("error:", e);
        }
    }

    private void forceRuleDataToLocal(TaskNode taskNode) throws Exception {
        SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(taskNode.getSchema());
        TableConfig tableConfig = schemaConfig.getTables().get(taskNode.getTable().toUpperCase());
        RuleConfig ruleConfig = tableConfig.getRule();
        String ruleName = ((TableRuleAware) ruleConfig.getRuleAlgorithm()).getRuleName() + ".properties";
        String rulePath = ZKUtils.getZKBasePath() + "ruledata/" + ruleName;
        CuratorFramework zk = ZKUtils.getConnection();
        byte[] ruleData = zk.getData().forPath(rulePath);
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的ruleData的信息--------------------------------------------------\n");
        LOGGER.info(new String(ruleData));
        Path file = Paths.get(SystemConfig.getHomePath(), "conf", "ruledata");
        Files.write(file.resolve(ruleName), ruleData);
    }

    private static void forceTableToLocal(Path localPath, XmlProcessBase xmlProcess) throws Exception {

        // 获得当前集群的名称
        String schemaPath = ZKUtils.getZKBasePath();
        schemaPath = schemaPath + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey() + ZookeeperPath.ZK_SEPARATOR.getKey();
        // 生成xml与类的转换信息
        SchemasParseXmlImpl schemasParseXml = new SchemasParseXmlImpl(xmlProcess);
        Schemas schema = new Schemas();
        String str = "";
        // 得到schema对象的目录信息
        str = new String(ZKUtils.getConnection().getData().forPath(schemaPath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey()));
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的schema的信息--------------------------------------------------");
        LOGGER.info(str);
        SchemaJsonParse schemaJsonParse = new SchemaJsonParse();
        List<Schema> schemaList = schemaJsonParse.parseJsonToBean(str);
        schema.setSchema(schemaList);
        // 得到dataNode的信息
        str = new String(ZKUtils.getConnection().getData().forPath(schemaPath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey()));
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的dataNode的信息--------------------------------------------------");
        LOGGER.info(str);
        DataNodeJsonParse dataNodeJsonParse = new DataNodeJsonParse();
        List<DataNode> dataNodeList = dataNodeJsonParse.parseJsonToBean(str);
        schema.setDataNode(dataNodeList);
        // 得到dataHost的信息
        str = new String(ZKUtils.getConnection().getData().forPath(schemaPath + ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey()));
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的dataHost的信息--------------------------------------------------");
        LOGGER.info(str);
        DataHostJsonParse dataHostJsonParse = new DataHostJsonParse();
        List<DataHost> dataHostList = dataHostJsonParse.parseJsonToBean(str);
        schema.setDataHost(dataHostList);

        xmlProcess.addParseClass(DataNode.class);
        xmlProcess.addParseClass(DataHost.class);
        xmlProcess.addParseClass(Schema.class);
        xmlProcess.addParseClass(Schemas.class);
        xmlProcess.initJaxbClass();

        schemasParseXml.parseToXmlWrite(schema, localPath.resolve("schema.xml").toString(), "schema");
    }

    private static void forceRulesToLocal(Path localPath, XmlProcessBase xmlProcess) throws Exception {
        Rules rules = new Rules();
        // tablerule信息
        String value = new String(ZKUtils.getConnection().getData().forPath(ZKUtils.getZKBasePath() + "rules/tableRule"), "UTF-8");
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的tablerule的信息--------------------------------------------------");
        LOGGER.info(value);
        DataInf RulesZkData = new ZkDataImpl("tableRule", value);
        TableRuleJsonParse tableRuleJsonParse = new TableRuleJsonParse();

        List<TableRule> tableRuleData = tableRuleJsonParse.parseJsonToBean(RulesZkData.getDataValue());
        rules.setTableRule(tableRuleData);
        // 得到function信息
        String fucValue = new String(ZKUtils.getConnection().getData().forPath(ZKUtils.getZKBasePath() + "rules/function"), "UTF-8");
        LOGGER.info("-----------------------------------从zookeeper中拉取的新的function的信息--------------------------------------------------");
        LOGGER.info(fucValue);
        DataInf functionZkData = new ZkDataImpl("function", fucValue);
        FunctionJsonParse functionJsonParse = new FunctionJsonParse();
        List<Function> functionList = functionJsonParse.parseJsonToBean(functionZkData.getDataValue());
        rules.setFunction(functionList);

        xmlProcess.addParseClass(Table.class);
        xmlProcess.addParseClass(Function.class);

        RuleParseXmlImpl ruleParseXml = new RuleParseXmlImpl(xmlProcess);
        xmlProcess.initJaxbClass();
        ruleParseXml.parseToXmlWrite(rules, localPath.resolve("rule.xml").toString(), "rule");
    }

    private void pushACKToClean(String taskPath) throws Exception {
        String myID = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
        String path = taskPath + "/_clean/" + myID;
        if (ZKUtils.getConnection().checkExists().forPath(path) == null) {
            ZKUtils.getConnection().create().creatingParentsIfNeeded().forPath(path);
        }
    }


    private void clean(String taskID, List<MigrateTask> allTaskList) throws IOException, SQLException {

        //clean
        for (MigrateTask migrateTask : allTaskList) {
            String sql = makeCleanSql(migrateTask);
            MigrateUtils.execulteSql(sql, migrateTask.getFrom());
        }
    }

    private void check(String taskID, List<MigrateTask> allTaskList) throws SQLException, IOException {
        for (MigrateTask migrateTask : allTaskList) {
            String sql = MigrateUtils.makeCountSql(migrateTask);
            long oldCount = MigrateUtils.execulteCount(sql, migrateTask.getFrom());
            long newCount = MigrateUtils.execulteCount(sql, migrateTask.getTo());
            if (oldCount != newCount) {
                throw new RuntimeException("migrate task (" + taskID + ") check fail,because  fromNode:"
                        + migrateTask.getFrom() + "(" + oldCount + ")" + " and  toNode:" + migrateTask.getTo() + "(" + newCount + ") and sql is " + sql);
            }
        }
    }

    private String makeCleanSql(MigrateTask task) {
        StringBuilder sb = new StringBuilder();
        sb.append("delete  from ");
        sb.append(task.getTable()).append(" where ");
        List<Range> slots = task.getSlots();
        for (int i = 0; i < slots.size(); i++) {
            Range range = slots.get(i);
            if (i != 0)
                sb.append(" or ");
            if (range.start == range.end) {
                sb.append(" _slot=").append(range.start);
            } else {
                sb.append("( _slot>=").append(range.start);
                sb.append(" and _slot<=").append(range.end).append(")");
            }
        }
        return sb.toString();
    }


    private CuratorTransactionFinal modifyRuleData(CuratorTransactionFinal transactionFinal, List<MigrateTask> allTaskList, TableConfig tableConfig, List<String> allNewDataNodes)
            throws Exception {

        InterProcessMutex ruleDataLock = null;
        try {
            String path = ZKUtils.getZKBasePath() + "lock/ruledata.lock";
            ruleDataLock = new InterProcessMutex(ZKUtils.getConnection(), path);
            ruleDataLock.acquire(30, TimeUnit.SECONDS);
            RuleConfig ruleConfig = tableConfig.getRule();
            String ruleName = ((TableRuleAware) ruleConfig.getRuleAlgorithm()).getRuleName() + ".properties";
            String rulePath = ZKUtils.getZKBasePath() + "ruledata/" + ruleName;
            CuratorFramework zk = ZKUtils.getConnection();
            byte[] ruleData = zk.getData().forPath(rulePath);
            Properties prop = new Properties();
            prop.load(new ByteArrayInputStream(ruleData));
            for (MigrateTask migrateTask : allTaskList) {
                modifyRuleData(prop, migrateTask, allNewDataNodes);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            prop.store(out, "WARNING   !!!Please do not modify or delete this file!!!");
            if (transactionFinal == null) {
                transactionFinal = ZKUtils.getConnection().inTransaction().setData().forPath(rulePath, out.toByteArray()).and();
            } else {
                transactionFinal.setData().forPath(rulePath, out.toByteArray());
            }
        } finally {
            try {
                if (ruleDataLock != null)
                    ruleDataLock.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return transactionFinal;
    }

    private void modifyRuleData(Properties prop, MigrateTask task, List<String> allNewDataNodes) {
        int fromIndex = -1;
        int toIndex = -1;
        List<String> dataNodes = allNewDataNodes;
        for (int i = 0; i < dataNodes.size(); i++) {
            String dataNode = dataNodes.get(i);
            if (dataNode.equalsIgnoreCase(task.getFrom())) {
                fromIndex = i;
            } else if (dataNode.equalsIgnoreCase(task.getTo())) {
                toIndex = i;
            }
        }
        String from = prop.getProperty(String.valueOf(fromIndex));
        String to = prop.getProperty(String.valueOf(toIndex));
        String fromRemain = removeRangesRemain(from, task.getSlots());
        String taskRanges = MigrateUtils.convertRangeListToString(task.getSlots());
        String newTo = to == null ? taskRanges : to + "," + taskRanges;
        prop.setProperty(String.valueOf(fromIndex), fromRemain);
        prop.setProperty(String.valueOf(toIndex), newTo);
    }

    private String removeRangesRemain(String ori, List<Range> rangeList) {
        List<Range> ranges = MigrateUtils.convertRangeStringToList(ori);
        List<Range> ramain = MigrateUtils.removeAndGetRemain(ranges, rangeList);
        return MigrateUtils.convertRangeListToString(ramain);
    }


    private static CuratorTransactionFinal modifyZkRules(CuratorTransactionFinal transactionFinal, String ruleName, List<String> newDataNodes)
            throws Exception {
        CuratorFramework client = ZKUtils.getConnection();
        String rulePath = ZKUtils.getZKBasePath() + "rules/function";
        JSONArray jsonArray = JSON.parseArray(new String(client.getData().forPath(rulePath), "UTF-8"));
        for (Object obj : jsonArray) {
            JSONObject func = (JSONObject) obj;
            if (ruleName.equalsIgnoreCase(func.getString("name"))) {
                JSONArray property = func.getJSONArray("property");
                for (Object o : property) {
                    JSONObject count = (JSONObject) o;
                    if ("count".equals(count.getString("name"))) {
                        Integer xcount = Integer.parseInt(count.getString("value"));
                        count.put("value", String.valueOf(xcount + newDataNodes.size()));

                        if (transactionFinal == null) {
                            transactionFinal = ZKUtils.getConnection().inTransaction().setData().forPath(rulePath, JSON.toJSONBytes(jsonArray)).and();
                        } else {
                            transactionFinal.setData().forPath(rulePath, JSON.toJSONBytes(jsonArray));
                        }
                    }
                }
            }

        }
        return transactionFinal;
    }

    private static CuratorTransactionFinal modifyTableConfigRules(CuratorTransactionFinal transactionFinal, String schemal, String table, List<String> newDataNodes)
            throws Exception {
        CuratorFramework client = ZKUtils.getConnection();
        String rulePath = ZKUtils.getZKBasePath() + "schema/schema";
        JSONArray jsonArray = JSON.parseArray(new String(client.getData().forPath(rulePath), "UTF-8"));
        for (Object obj : jsonArray) {
            JSONObject func = (JSONObject) obj;
            if (schemal.equalsIgnoreCase(func.getString("name"))) {

                JSONArray property = func.getJSONArray("table");
                for (Object o : property) {
                    JSONObject tt = (JSONObject) o;
                    String tableName = tt.getString("name");
                    String dataNode = tt.getString("dataNode");
                    if (table.equalsIgnoreCase(tableName)) {
                        List<String> allDataNodes = new ArrayList<>();
                        allDataNodes.add(dataNode);
                        allDataNodes.addAll(newDataNodes);
                        tt.put("dataNode", Joiner.on(",").join(allDataNodes));
                        if (transactionFinal == null) {
                            transactionFinal = ZKUtils.getConnection().inTransaction().setData().forPath(rulePath, JSON.toJSONBytes(jsonArray)).and();
                        } else {
                            transactionFinal.setData().forPath(rulePath, JSON.toJSONBytes(jsonArray));
                        }
                    }

                }
            }
        }
        return transactionFinal;
    }
}
