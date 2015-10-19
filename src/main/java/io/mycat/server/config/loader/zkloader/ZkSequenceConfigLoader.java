package io.mycat.server.config.loader.zkloader;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by v1.lion on 2015/10/11.
 */
public class ZkSequenceConfigLoader extends AbstractZKLoaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkSequenceConfigLoader.class);

    //directory name of rule node config in zookeeper
    private static final String SEQUENCE_CONFIG_DIRECTORY = "sequence-config";

    public ZkSequenceConfigLoader(final String clusterID) {
        super(clusterID, SEQUENCE_CONFIG_DIRECTORY);
    }

    @Override
    public void fetchConfig(final CuratorFramework zkConnection) {

    }

}
