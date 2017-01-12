package io.mycat.config.loader.zkprocess.zookeeper;

/**
 * Created by magicdoom on 2016/12/21.
 */
public class ClusterInfo {
    private int clusterSize;
    private String clusterNodes;


    public int getClusterSize() {
        return clusterSize;
    }

    public void setClusterSize(int clusterSize) {
        this.clusterSize = clusterSize;
    }

    public String getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }


}
