package io.mycat.route.sequence.handler;

import io.mycat.config.model.SystemConfig;
import io.mycat.route.util.PropertiesUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 基于ZK与本地配置的分布式ID生成器(可以通过ZK获取集群（机房）唯一InstanceID，也可以通过配置文件配置InstanceID)
 * ID结构：long 64位，ID最大可占63位
 * |current time millis(微秒时间戳41位)|clusterId（机房或者ZKid，通过配置文件配置4位）|instanceId（实例ID，可以通过ZK或者配置文件获取，4位）|threadId（线程ID，7位）|increment(自增,7位)
 * 一共63位，可以承受单机房单机器单线程1000*(2^7)=1280000的并发。
 * 无悲观锁，无强竞争，吞吐量更高
 * <p/>
 * 配置文件：sequence_distributed_conf.properties
 * 只要配置里面：INSTANCEID=ZK就是从ZK上获取InstanceID
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 00:08:03 2016/5/3
 */
public class DistributedSequenceHandler extends LeaderSelectorListenerAdapter implements Closeable, SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory.getLogger(DistributedSequenceHandler.class);
    private static final String SEQUENCE_DB_PROPS = "sequence_distributed_conf.properties";
    private static DistributedSequenceHandler instance;

    public static DistributedSequenceHandler getInstance(SystemConfig systemConfig) {
        if (instance == null) {
            instance = new DistributedSequenceHandler(systemConfig);
        }
        return instance;
    }

    private long timestampBits = 41L;
    private long clusterIdBits = 4L;
    private long instanceIdBits = 4L;
    private long threadIdBits = 7L;
    private long incrementBits = 7L;

    private long incrementShift = 0L;
    private long threadIdShift = incrementShift + incrementBits;
    private long instanceIdShift = threadIdShift + threadIdBits;
    private long clusterIdShift = instanceIdShift + instanceIdBits;
    private long timestampShift = clusterIdShift + clusterIdBits;

    private long maxIncrement = 1 << incrementBits;
    private long maxThreadId = 1 << threadIdBits;
    private long maxinstanceId = 1 << instanceIdBits;
    private long maxclusterId = 1 << instanceIdBits;

    private volatile long instanceId;
    private long clusterId;

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    private final static String PATH = "/mycat/sequence";
    private SystemConfig mycatConfig;
    private String ID;

    private int mark[];
    private volatile boolean isLeader = false;
    //配置是否载入好
    private volatile boolean ready = false;

    private CuratorFramework client;
    private PathChildrenCache cache;
    private LeaderSelector leaderSelector;

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public LeaderSelector getLeaderSelector() {
        return leaderSelector;
    }

    public void setLeaderSelector(LeaderSelector leaderSelector) {
        this.leaderSelector = leaderSelector;
    }

    private ConcurrentMap<Thread, Integer> threadInc = new ConcurrentHashMap<Thread, Integer>((int) maxIncrement);

    public DistributedSequenceHandler(SystemConfig mycatConfig) {
        this.mycatConfig = mycatConfig;
        ID = mycatConfig.getBindIp() + mycatConfig.getServerPort();
    }

    public void load() {
        // load sequnce properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS);
        if ("ZK".equals(props.getProperty("INSTANCEID"))) {
            initializeZK(props.getProperty("ZK"));
        } else {
            this.instanceId = Long.valueOf(props.getProperty("INSTANCEID"));
            this.ready = true;
        }
        this.clusterId = Long.valueOf(props.getProperty("CLUSTERID"));


    }

    public void initializeZK(String zkAddress) {
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        this.leaderSelector = new LeaderSelector(client, PATH + "/leader", this);
        this.leaderSelector.autoRequeue();
        this.leaderSelector.start();
        new Thread() {
            @Override
            public void run() {
                while (!leaderExists()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Unexpected thread interruption!");
                    }
                }
                while (true) {
                    if (isLeader) {
                        continue;
                    }
                    while (!tryGetInstanceID()) ;
                    //心跳，需要考虑网络不通畅时，别人抢占了自己的节点，需要重新获取
                    while (true) {
                        try {
                            if (isLeader) break;
                            byte[] data = client.getData().forPath(PATH + "/instance/" + instanceId);
                            if (data == null || !new String(data).equals(ID)) {
                                while (!tryGetInstanceID()) ;
                            }
                            //每隔10秒一次心跳
                            Thread.sleep(10000);
                        } catch (Exception e) {
                            LOGGER.warn("Exception caught:" + e.getCause());
                        }
                    }
                }
            }
        }.start();
    }

    private boolean tryGetInstanceID() {
        try {
            this.ready = false;
            byte[] data = this.client.getData().forPath(PATH + "/next");
            String nextCounter = new String(data);
            this.instanceId = Integer.parseInt(nextCounter);
            this.client.create().withMode(CreateMode.EPHEMERAL).forPath(PATH + "/instance/" + this.instanceId, ID.getBytes());
            this.ready = true;
            return true;
        } catch (Exception e) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                LOGGER.warn("Unexpected thread interruption!");
            }
            LOGGER.warn("Exception caught while trying to get InstanceID from ZK!If this exception frequently happens, please check your network connection!" + e.getCause());
            return false;
        }
    }

    private boolean leaderExists() {
        try {
            List<String> children = this.client.getChildren().forPath(PATH + "/leader");
            if (children.size() <= 0) {
                return false;
            } else {
                if (this.leaderSelector.getLeader() == null) {
                    return false;
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
    }

    private Set<Long> isSet = new HashSet<>();

    @Override
    public long nextId(String prefixName) {
        while (!ready) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.warn("Unexpected thread interruption!");
            }
        }
        final Thread thread = Thread.currentThread();
        if (threadInc.get(thread) == null) {
            threadInc.put(thread, 0);
        } else {
            int a = threadInc.get(thread);
            if (a >= maxIncrement) {
                threadInc.put(thread, 0);
            } else {
                threadInc.put(thread, a + 1);
            }
        }
        int a = threadInc.get(thread);
        return (System.currentTimeMillis() << timestampShift) | (((thread.getId() % maxThreadId) << threadIdShift)) | (instanceId << instanceIdShift) | (clusterId << clusterIdShift) | a;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
            this.isLeader = false;
            throw new CancelLeadershipException();
        }
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) {
        cache = null;
        try {
            this.isLeader = true;

            if (this.client.checkExists().forPath(PATH + "/instance/" + instanceId) == null)
                this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(PATH + "/instance/" + instanceId, ID.getBytes());

            cache = new PathChildrenCache(client, PATH + "/instance", true);
            this.mark = new int[(int) maxinstanceId];
            PathChildrenCacheListener listener = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework c, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                        case CHILD_ADDED: {
                            LOGGER.debug("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            reloadChild();
                            client.setData().forPath(PATH + "/next", ("" + nextFree()).getBytes());
                            break;
                        }

                        case CHILD_UPDATED: {
                            LOGGER.debug("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            client.setData().forPath(PATH + "/next", ("" + nextFree()).getBytes());
                            reloadChild();
                            break;
                        }

                        case CHILD_REMOVED: {
                            LOGGER.debug("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            client.setData().forPath(PATH + "/next", ("" + nextFree()).getBytes());
                            reloadChild();
                            break;
                        }
                    }
                }
            };
            cache.getListenable().addListener(listener);
            cache.start();
            reloadChild();
            this.client.create().withMode(CreateMode.EPHEMERAL).forPath(PATH + "/next", ("" + nextFree()).getBytes());
            this.ready = true;
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.warn("Unexpected thread interruption!");
        } catch (Exception e) {
            LOGGER.warn("Exception caught:" + e.getCause());
        } finally {
            CloseableUtils.closeQuietly(cache);
        }
    }

    private void reloadChild() throws Exception {
        mark = new int[(int) maxinstanceId];
        List<String> list = client.getChildren().forPath(PATH + "/instance");
        for (String child : list) {
            mark[Integer.parseInt(child)] = 1;
        }

    }

    private int nextFree() {
        for (int i = 0; i < mark.length; i++) {
            if (mark[i] != 1)
                return i;
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(this.leaderSelector);
        CloseableUtils.closeQuietly(this.cache);
        CloseableUtils.closeQuietly(this.client);
    }

}
