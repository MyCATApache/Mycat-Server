/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server.config.loader;

import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.node.CharsetConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.DataNodeConfig;
import io.mycat.server.config.node.HostIndexConfig;
import io.mycat.server.config.node.QuarantineConfig;
import io.mycat.server.config.node.RuleConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SequenceConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.UserConfig;

import java.util.Map;

/**
 * @author mycat
 */
public interface ConfigLoader {
	SchemaConfig getSchemaConfig(String schema);
	Map<String, SchemaConfig> getSchemaConfigs();
	Map<String, DataNodeConfig> getDataNodeConfigs();
	Map<String, DataHostConfig> getDataHostConfigs();
	Map<String, RuleConfig> getTableRuleConfigs();
	SystemConfig getSystemConfig();
	UserConfig getUserConfig(String user);
	Map<String, UserConfig> getUserConfigs();
	QuarantineConfig getQuarantineConfigs();
	MycatClusterConfig getClusterConfigs();
	CharsetConfig getCharsetConfigs();
	HostIndexConfig getHostIndexConfig();
	SequenceConfig getSequenceConfig();

}