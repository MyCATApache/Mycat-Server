package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.RuleConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.common.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by v1.lion on 2015/10/8.
 */
public class ZkRuleConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkRuleConfigLoader.class);

    //directory name of rule node config in zookeeper
    private static final String RULE_CONFIG_DIRECTORY = "rule-config";
    private static final String RULE_NAME_KEY = "name";
    private static final String FUNCTION_NAME_KEY = "functionName";
    private static final String COLUMN_NAME_KEY = "column";

    //hold rule name mapping to RuleConfig
    private Map<String, RuleConfig> ruleConfigMap;


    public ZkRuleConfigLoader(final String clusterID) {
        super(clusterID, RULE_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        //rule config path in zookeeper
        //example: /mycat-cluster-1/rule-config/sharding-by-enum
        final List<String> ruleNodeNames;
        try {
            ruleNodeNames = zkConnection
                    .getChildren()
                    .forPath(BASE_CONFIG_PATH);
        } catch (Exception e) {
            LOGGER.error("fetch rule child node from zookeeper error : {} , path {} ", e.getMessage(), BASE_CONFIG_PATH);
            throw new ConfigException(e);
        }

        ruleNodeNames
                .stream()
                .forEach(
                        nodeName -> {
                            //fetch data
                            String rawRuleStr;
                            String ruleNodePath = ZKPaths.makePath(BASE_CONFIG_PATH, nodeName);
                            try {
                                byte[] rawByte = zkConnection
                                        .getData()
                                        .forPath(ruleNodePath);
                                rawRuleStr = new String(rawByte, StandardCharsets.UTF_8);
                                LOGGER.trace("get raw data from zookeeper: {}", rawRuleStr);
                            } catch (Exception e) {
                                LOGGER.error("get rule config data from zookeeper error : {}, path : {}", e.getMessage(), ruleNodePath);
                                throw new ConfigException(e);
                            }

                            //parse
                            JSONObject ruleJson = JSON.parseObject(rawRuleStr);
                            String ruleName = ruleJson.getString(RULE_NAME_KEY);
                            String functionName = ruleJson.getString(FUNCTION_NAME_KEY);
                            String columnName = ruleJson.getString(COLUMN_NAME_KEY);

                            //create RuleConfig
                            RuleConfig ruleConfig = new RuleConfig(ruleName, functionName, columnName);
                            AbstractPartitionAlgorithm ruleFunction = instanceFunction(ruleName, functionName);

                            //for bean copy
                            ruleJson.remove("name");
                            ruleJson.remove("functionName");
                            ruleJson.remove("column");
                            try {
                                BeanUtils.populate(ruleConfig, ruleJson);
                            } catch (IllegalAccessException | InvocationTargetException e) {
                                throw new ConfigException("copy property to " + functionName + " error: ", e);
                            }

                            ruleConfig.setRuleAlgorithm(ruleFunction);
                            ruleConfig.setProps(ruleJson);
                        }
                );
    }

    private AbstractPartitionAlgorithm instanceFunction(String name, String clazz) {
        try {
            Class<?> clz = Class.forName(clazz);
            if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
                throw new IllegalArgumentException("rule function must implements "
                        + AbstractPartitionAlgorithm.class.getName() + ", name=" + name);
            }
            return (AbstractPartitionAlgorithm) clz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.warn(e.getMessage(), e);
            throw new ConfigException(e);
        }
    }

    public Map<String, RuleConfig> getRuleConfigs() {
        return this.ruleConfigMap;
    }
}
