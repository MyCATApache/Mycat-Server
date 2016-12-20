package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteCheckRule;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by magicdoom on 2016/12/19.
 */
public class SwitchPrepareListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateMainRunner.class);
    @Override public void childEvent(CuratorFramework curatorFramework,
            PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:
                 checkSwitch(event);
                break;
            default:
                break;
        }
    }
    private void checkSwitch(PathChildrenCacheEvent event)    {
        InterProcessMutex taskLock =null;
        try {

            String path=event.getData().getPath();
            String taskPath=path.substring(0,path.lastIndexOf("/_prepare/"))  ;
            String taskID=taskPath.substring(taskPath.lastIndexOf('/')+1,taskPath.length());
            String lockPath=     ZKUtils.getZKBasePath()+"lock/"+taskID+".lock";
            List<String> sucessDataHost= ZKUtils.getConnection().getChildren().forPath(path.substring(0,path.lastIndexOf('/')));
            List<MigrateTask> allTaskList=MigrateUtils.queryAllTask(path.substring(0,path.lastIndexOf('/')),sucessDataHost);
            TaskNode pTaskNode= JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath),TaskNode.class);

            ConcurrentMap<String,List<PartitionByCRC32PreSlot.Range>> tableRuleMap=
                    RouteCheckRule.migrateRuleMap.containsKey(pTaskNode.getSchema().toUpperCase()) ?
                    RouteCheckRule.migrateRuleMap.get(pTaskNode.getSchema().toUpperCase()) :
                    new ConcurrentHashMap();
                 tableRuleMap.put(pTaskNode.getTable().toUpperCase(),MigrateUtils.convertAllTask(allTaskList));
            RouteCheckRule.migrateRuleMap.put(pTaskNode.getSchema().toUpperCase(),tableRuleMap);


            taskLock=	 new InterProcessMutex(ZKUtils.getConnection(), lockPath);
            taskLock.acquire(20, TimeUnit.SECONDS);

            List<String> dataHost=  ZKUtils.getConnection().getChildren().forPath(taskID) ;
            if(getRealSize(dataHost)==sucessDataHost.size()){
                TaskNode taskNode= JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath),TaskNode.class);
                  if(taskNode.getStatus()==1){
                       taskNode.setStatus(2);  //prepare switch
                      ZKUtils.getConnection().setData().forPath(taskPath,JSON.toJSONBytes(taskNode))  ;
                  }
              }
        } catch (Exception e) {
            LOGGER.error("error:",e);
        }
        finally {
            if(taskLock!=null){
                try {
                    taskLock.release();
                } catch (Exception ignored) {

                }
            }
        }
    }

    private int getRealSize(List<String> dataHosts){
        int size=dataHosts.size();
        Set<String> set=new HashSet(dataHosts);
        if(set.contains("_prepare"))   {
            size=size-1;
        }
        if(set.contains("_commit"))   {
            size=size-1;
        }

        return size;
    }
    private void checkSwitch1(PathChildrenCacheEvent event)    {
        InterProcessMutex taskLock =null;
        try {

            String path=event.getData().getPath();
            String taskPath=path.substring(0,path.lastIndexOf("/_status/"))  ;
            String taskID=taskPath.substring(taskPath.lastIndexOf('/')+1,taskPath.length());
            String lockPath=     ZKUtils.getZKBasePath()+"lock/"+taskID+".lock";
            taskLock=	 new InterProcessMutex(ZKUtils.getConnection(), lockPath);
            taskLock.acquire(20, TimeUnit.SECONDS);
            List<String> sucessDataHost= ZKUtils.getConnection().getChildren().forPath(path.substring(0,path.lastIndexOf('/')));
            List<String> dataHost=  ZKUtils.getConnection().getChildren().forPath(taskID) ;
            if(dataHost.size()-1==sucessDataHost.size()){
                   CuratorTransactionFinal transactionFinal=null;
                TaskNode taskNode= JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath),TaskNode.class);

                SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(taskNode.getSchema());
                TableConfig tableConfig = schemaConfig.getTables().get(taskNode.getTable().toUpperCase());
                List<String> newDataNodes = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(taskNode.getAdd());

                            //先修改rule config
                             InterProcessMutex  ruleLock = new InterProcessMutex(ZKUtils.getConnection(), ZKUtils.getZKBasePath()+"lock/rules.lock");;
                            try {
                                ruleLock.acquire(30, TimeUnit.SECONDS);
                               modifyZkRules(transactionFinal,tableConfig.getRule().getFunctionName(),newDataNodes);
                                modifyTableConfigRules(transactionFinal,taskNode.getSchema(),taskNode.getTable(),newDataNodes);
                            }
                            finally {
                                ruleLock.release();
                            }


                transactionFinal.commit() ;
            }
        } catch (Exception e) {
           LOGGER.error("error:",e);
        }   finally {
            if(taskLock!=null){
                try {
                    taskLock.release();
                } catch (Exception ignored) {

                }
            }
        }
    }




    private void xx()
    {
//                byte[] data=   ZKUtils.getConnection().getData().forPath(zkPath) ;
//                TaskStatus taskStatus =JSON.parseObject(data, TaskStatus.class);
//                if(taskStatus.getStatus()==1)  {
//                    taskStatus.setStatus(3);
//                    migrateTask.setStatus(3);
//                    ZKUtils.getConnection().setData().forPath(zkPath,JSON.toJSONBytes(taskStatus)) ;
//                    InterProcessMutex ruleDataLock =null;
//                    try {
//                        String path=     ZKUtils.getZKBasePath()+"lock/ruledata.lock";
//                        ruleDataLock=	 new InterProcessMutex(ZKUtils.getConnection(), path);
//                        ruleDataLock.acquire(30, TimeUnit.SECONDS);
//                        Map<String, SchemaConfig> schemaConfigMap= MycatServer.getInstance().getConfig().getSchemas() ;
//                        SchemaConfig schemaConfig=   schemaConfigMap.get(migrateTask.getSchema());
//                        TableConfig tableConfig = schemaConfig.getTables()
//                                .get(migrateTask.getTable().toUpperCase());
//                        RuleConfig ruleConfig= tableConfig.getRule();
//                        String ruleName=ruleConfig.getFunctionName()+"_"+ migrateTask.getTable().toUpperCase()+".properties";
//                        String rulePath=ZKUtils.getZKBasePath()+"ruledata/"+ruleName;
//                        CuratorFramework zk = ZKUtils.getConnection();
//                        byte[] ruleData=zk.getData().forPath(rulePath);
//                        Properties prop = new Properties();
//                        prop.load(new ByteArrayInputStream(ruleData));
//                        modifyRuleData(prop,migrateTask,tableConfig);
//                        ByteArrayOutputStream out=new ByteArrayOutputStream();
//                        prop.store(out, "WARNING   !!!Please do not modify or delete this file!!!");
//                        zk.setData().forPath(ruleName, out.toByteArray());
//
//                    } finally {
//                        try {
//                            if(ruleDataLock!=null)
//                                ruleDataLock.release();
//                        } catch (Exception e) {
//                            throw new RuntimeException(e);
//                        }
//                    }
//                }
    }

    private   void modifyRuleData( Properties prop ,MigrateTask task, TableConfig tableConfig ){
        int fromIndex=-1;
        int toIndex=-1;
        List<String> dataNodes=   tableConfig.getDataNodes();
        for (int i = 0; i < dataNodes.size(); i++) {
            String dataNode = dataNodes.get(i);
            if(dataNode.equalsIgnoreCase(task.getFrom())){
                fromIndex=i;
            } else
            if(dataNode.equalsIgnoreCase(task.getTo())){
                toIndex=i;
            }
        }
        String from=  prop.getProperty(String.valueOf(fromIndex)) ;
    }


    private static void modifyZkRules( CuratorTransactionFinal transactionFinal,String ruleName ,List<String> newDataNodes )
            throws Exception {
        CuratorFramework client= ZKUtils.getConnection();
        String rulePath= ZKUtils.getZKBasePath() + "rules/function";
        JSONArray jsonArray= JSON.parseArray(new String(client.getData().forPath(rulePath) ,"UTF-8"))  ;
        for (Object obj: jsonArray) {
            JSONObject func= (JSONObject) obj;
            if(ruleName.equalsIgnoreCase(func.getString("name"))) {
                JSONArray property=   func.getJSONArray("property") ;
                for (Object o : property) {
                    JSONObject count= (JSONObject) o;
                    if("count".equals(count.getString("name"))){
                        Integer xcount=Integer.parseInt( count.getString("value")) ;
                        count.put("value",String.valueOf(xcount+newDataNodes.size())) ;
                        transactionFinal.setData().forPath(rulePath,JSON.toJSONBytes(jsonArray)) ;
                    }
                }
            }

        }
    }

    private static void modifyTableConfigRules( CuratorTransactionFinal transactionFinal,String schemal,String table ,List<String> newDataNodes )
            throws Exception {
        CuratorFramework client= ZKUtils.getConnection();
        String rulePath= ZKUtils.getZKBasePath() + "schema/schema";
        JSONArray jsonArray= JSON.parseArray(new String(client.getData().forPath(rulePath) ,"UTF-8"))  ;
        for (Object obj: jsonArray) {
            JSONObject func= (JSONObject) obj;
            if(schemal.equalsIgnoreCase(func.getString("name"))) {

                JSONArray property = func.getJSONArray("table");
                for (Object o : property) {
                    JSONObject tt= (JSONObject) o;
                    String tableName = tt.getString("name");
                    String dataNode = tt.getString("dataNode");
                    if (table.equalsIgnoreCase(tableName)) {
                        List<String> allDataNodes = new ArrayList<>();
                        allDataNodes.add(dataNode);
                        allDataNodes.addAll(newDataNodes);
                        tt.put("dataNode", Joiner.on(",").join(allDataNodes));
                        transactionFinal.setData().forPath(rulePath, JSON.toJSONBytes(jsonArray));
                    }

                }
            }
        }
    }
}
