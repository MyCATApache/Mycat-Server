package io.mycat.server.config.loader.zkloader;

import com.alibaba.fastjson.JSON;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;

/**
 * Created by v1.lion on 2015/10/18.
 */
public class ZkDataHostConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDataHostConfigLoader.class);

    private static final String DATAHOST_CONFIG_DIRECTORY = "datahost-config";

    //hold datahost name mapping to DataHostConfig
    private Map<String, DataHostConfig> dataHostConfigs;

    private CuratorFramework zkConnection;

    public ZkDataHostConfigLoader(final String clusterID) {
        super(clusterID, DATAHOST_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(CuratorFramework zkConnection) {
        this.zkConnection = zkConnection;

        /// mycat-cluster-1/ datahost -config/ ${dataHostName}
        dataHostConfigs = super.fetchChildren(this.zkConnection, "")
                .stream()
                .map(dataHostName -> createHostConfig(dataHostName))
                .collect(Collectors.toMap(DataHostConfig::getName, Function.identity()));

        this.zkConnection = null;
    }

    private DataHostConfig createHostConfig(final String dataHostName) {
        //parse dataHost
        DataHostConfig dataHostConfig = JSON.parseObject(
                super.fetchData(this.zkConnection, dataHostName), DataHostConfig.class);
        //for put read host
        dataHostConfig.setReadHosts(new HashMap<>());

        //create write host and read host
        AtomicInteger writeCount = new AtomicInteger();
        List<DBHostConfig> writeHostList = new ArrayList<>();

        /// mycat-cluster-1/ datahost -config/ ${dataHostName} / ${writeHostName}
        super.fetchChildren(this.zkConnection, dataHostName)
                .stream()
                .forEach(writeHostName -> {
                    int writeIndex = writeCount.getAndIncrement();
                    writeHostList.add(generateWriteHostConfig(dataHostConfig, dataHostName, writeHostName));
                    buildReadHostConfig(dataHostConfig, dataHostName, writeHostName, writeIndex);
                });


        //Convert list to array
        DBHostConfig[] writeArray = new DBHostConfig[writeHostList.size()];
        writeHostList.toArray(writeArray);

        dataHostConfig.setWriteHosts(writeArray);
        return dataHostConfig;
    }

    private void buildReadHostConfig(DataHostConfig dataHostConfig, String dataHost,
                                     String writeHostName, int writeIndex) {
        List<DBHostConfig> readHostList = new ArrayList<>();

        //parse read host
        super.fetchChildren(this.zkConnection, dataHost, writeHostName)
                .stream()
                .forEach(readHostName -> {
                    DBHostConfig readHostConfig = JSON.parseObject(
                            super.fetchData(this.zkConnection, dataHost, writeHostName, readHostName)
                            , DBHostConfig.class);
                    readHostConfig.setDbType(dataHostConfig.getDbType());
                    readHostConfig.setMaxCon(dataHostConfig.getMaxCon());
                    readHostConfig.setMinCon(dataHostConfig.getMinCon());

                    readHostList.add(readHostConfig);
                    LOGGER.trace("generate read host config : {}", readHostConfig);
                });

        if (readHostList.size() > 0) {
            //Convert list to array
            DBHostConfig[] readArray = new DBHostConfig[readHostList.size()];
            readHostList.toArray(readArray);

            //set to dataHostConfig
            dataHostConfig.getReadHosts().put(writeIndex, readArray);
        }
    }

    private DBHostConfig generateWriteHostConfig(DataHostConfig dataHostConfig, String dataHost,
                                                 String writeHostName) {
        //parse write host
        DBHostConfig writeHostConfig = JSON.parseObject(
                super.fetchData(this.zkConnection, dataHost, writeHostName), DBHostConfig.class);

        writeHostConfig.setDbType(dataHostConfig.getDbType());
        writeHostConfig.setDbType(dataHostConfig.getDbType());
        writeHostConfig.setMaxCon(dataHostConfig.getMaxCon());
        writeHostConfig.setMinCon(dataHostConfig.getMinCon());

        LOGGER.trace("generate write host config : {}", writeHostConfig);
        return writeHostConfig;
    }

    public Map<String, DataHostConfig> getDataHostConfigs() {
        return dataHostConfigs;
    }
}
