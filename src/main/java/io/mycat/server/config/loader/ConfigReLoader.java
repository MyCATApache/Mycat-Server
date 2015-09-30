package io.mycat.server.config.loader;


public interface ConfigReLoader {
	void reloadSchemaConfig(String schema);
	void reloadSchemaConfigs();
	void reloadDataNodeConfigs();
	void reloadDataHostConfigs();
	void reloadTableRuleConfigs();
	void reloadSystemConfig();
	void reloadUserConfig(String user);
	void reloadUserConfigs();
	void reloadQuarantineConfigs();
	void reloadClusterConfigs();
	void reloadCharsetConfigs();
	void reloadHostIndexConfig();

}
