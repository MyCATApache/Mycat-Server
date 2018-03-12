package io.mycat.config.loader.zkprocess.zktoxml.listen;

import com.google.common.io.Files;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.ReloadFunction;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by magicdoom on 2016/10/27.
 */
public class RuleDataPathChildrenCacheListener implements PathChildrenCacheListener {
    @Override public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        ChildData data = event.getData();
        switch (event.getType()) {

            case CHILD_ADDED:

                add(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()) ;
                break;
            case CHILD_REMOVED:
                delete(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()); ;
                break;
            case CHILD_UPDATED:
                add(data.getPath().substring(data.getPath().lastIndexOf("/")+1),event.getData().getData()) ;
                break;
            default:
                break;
        }
    }



    private void reloadRuleData(String name){
        String tableName=name.substring(name.lastIndexOf("_")+1,name.indexOf("."));
        String ruleName=name.substring(0,name.indexOf("."));
        Map<String, SchemaConfig> schemaConfigMap= MycatServer.getInstance().getConfig().getSchemas() ;
        for (SchemaConfig schemaConfig : schemaConfigMap.values()) {
            TableConfig tableConfig= schemaConfig.getTables().get(tableName.toUpperCase());
            if(tableConfig==null)continue;
            RuleConfig rule=tableConfig.getRule();
            AbstractPartitionAlgorithm function= rule.getRuleAlgorithm() ;
            if(function instanceof ReloadFunction){
                ((ReloadFunction) function).reload();
            }
        }
    }

    private void add(String name,byte[] data) throws IOException {
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "ruledata",
                name);
        Files.write(data,file);
        reloadRuleData(name);
    }

    private void delete(String name,byte[] data) throws IOException {
        File file = new File(
                SystemConfig.getHomePath() + File.separator + "conf" + File.separator + "ruledata",
                name);
        if(file.exists())
         file.delete();
    }

}
