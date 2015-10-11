package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.node.RuleConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

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
        this.ruleConfigMap = super
                .fetchChildren(zkConnection)
                .stream()
                .map(nodeName -> {
                    //fetch data
                    String rawRuleStr = super.fetchDataToString(zkConnection, nodeName);

                    //parse
                    JSONObject ruleJson = JSON.parseObject(rawRuleStr);

                    //create RuleConfig
                    RuleConfig ruleConfig = new RuleConfig(
                            ruleJson.getString(RULE_NAME_KEY),
                            ruleJson.getString(COLUMN_NAME_KEY),
                            ruleJson.getString(FUNCTION_NAME_KEY));

                    AbstractPartitionAlgorithm ruleFunction = instanceFunction(ruleJson);

                    ruleConfig.setRuleAlgorithm(ruleFunction);
                    ruleConfig.setProps(ruleJson);
                    return ruleConfig;
                })
                .collect(toMap(RuleConfig::getName, Function.identity()));
        LOGGER.trace("done fetch rule config : {}", ruleConfigMap);
    }

    private AbstractPartitionAlgorithm instanceFunction(JSONObject ruleJson) {
        String functionName = ruleJson.getString(FUNCTION_NAME_KEY);
        String ruleName = ruleJson.getString(RULE_NAME_KEY);

        //for bean copy
        ruleJson.remove(COLUMN_NAME_KEY);
        ruleJson.remove(FUNCTION_NAME_KEY);
        ruleJson.remove(RULE_NAME_KEY);

        AbstractPartitionAlgorithm algorithm;
        try {
            Class<?> clz = Class.forName(functionName);
            if (!AbstractPartitionAlgorithm.class.isAssignableFrom(clz)) {
                throw new IllegalArgumentException("rule function must implements "
                        + AbstractPartitionAlgorithm.class.getName() + ", name=" + ruleName);
            }
            algorithm = (AbstractPartitionAlgorithm) clz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.warn("instance function class error: {}", e.getMessage(), e);
            throw new ConfigException(e);
        }

        try {
            BeanUtils.populate(algorithm, ruleJson);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new ConfigException("copy property to " + functionName + " error: ", e);
        }

        //init
        algorithm.init();
        LOGGER.trace("instanced function class : {}", functionName);
        return algorithm;
    }

    public Map<String, RuleConfig> getRuleConfigs() {
        return this.ruleConfigMap;
    }
}
