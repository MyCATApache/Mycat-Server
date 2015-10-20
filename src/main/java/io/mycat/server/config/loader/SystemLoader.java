package io.mycat.server.config.loader;

import io.mycat.server.config.node.SystemConfig;

/**
 * Instances of this interface maintain the configuration of system.
 * The Configuration is described by an instance of SystemConfig.
 * Created by v1.lion on 2015/9/27.
 */
public interface SystemLoader {
    SystemConfig getSystemConfig();
}
