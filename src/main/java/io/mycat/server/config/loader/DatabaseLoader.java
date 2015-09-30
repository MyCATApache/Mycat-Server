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

public class DatabaseLoader  implements ConfigLoader {

	@Override
	public SchemaConfig getSchemaConfig(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, SchemaConfig> getSchemaConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DataNodeConfig> getDataNodeConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DataHostConfig> getDataHostConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, RuleConfig> getTableRuleConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SystemConfig getSystemConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserConfig getUserConfig(String user) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, UserConfig> getUserConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public QuarantineConfig getQuarantineConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MycatClusterConfig getClusterConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CharsetConfig getCharsetConfigs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HostIndexConfig getHostIndexConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SequenceConfig getSequenceConfig() {
		// TODO Auto-generated method stub
		return null;
	}

}
