package io.mycat.server.config.loader.zkloader;

/**
 * zookeeper config bean
 * Created by v1.lion on 2015/10/6.
 */
public class ZkConfig {
    private String zkURL;
    private String clusterID;
    private String myID;

    public ZkConfig() {
        super();
    }

    public String getZkURL() {
        return zkURL;
    }

    public void setZkURL(String zkURL) {
        this.zkURL = zkURL;
    }

    public String getClusterID() {
        return clusterID;
    }

    public void setClusterID(String clusterID) {
        this.clusterID = clusterID;
    }

    public String getMyID() {
        return myID;
    }

    public void setMyID(String myID) {
        this.myID = myID;
    }

    @Override
    public String toString() {
        return "ZkConfig{" +
                "zkURL='" + zkURL + '\'' +
                ", clusterID='" + clusterID + '\'' +
                ", myID='" + myID + '\'' +
                '}';
    }
}
