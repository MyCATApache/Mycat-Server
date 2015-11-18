package io.mycat.server.config.cluster;

public interface ClusterSync {

	public void init();
    public boolean switchDataSource(String dataHost, int curIndex);
}
