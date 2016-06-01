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
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于ZK与本地配置的分布式ID生成器(可以通过ZK获取集群（机房）唯一InstanceID，也可以通过配置文件配置InstanceID)
 * ID结构：long 64位，ID最大可占63位
 * |current time millis(微秒时间戳38位,可以使用17年)|clusterId（机房或者ZKid，通过配置文件配置5位）|instanceId（实例ID，可以通过ZK或者配置文件获取，5位）|threadId（线程ID，9位）|increment(自增,6位)
 * 一共63位，可以承受单机房单机器单线程1000*(2^6)=640000的并发。
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

    private final long timestampBits = 38L;
    private final long clusterIdBits = 5L;
    private final long instanceIdBits = 5L;
    private final long threadIdBits = 9L;
    private final long incrementBits = 6L;

    private final long timestampMask = (1L << timestampBits) - 1L;

    private final long incrementShift = 0L;
    private final long threadIdShift = incrementShift + incrementBits;
    private final long instanceIdShift = threadIdShift + threadIdBits;
    private final long clusterIdShift = instanceIdShift + instanceIdBits;
    private final long timestampShift = clusterIdShift + clusterIdBits;

    private final long maxIncrement = 1L << incrementBits;
    private final long maxThreadId = 1L << threadIdBits;
    private final long maxinstanceId = 1L << instanceIdBits;
    private final long maxclusterId = 1L << instanceIdBits;

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

    private ThreadLocal<Long> threadInc = new ThreadLocal<>();
    private ThreadLocal<Long> threadLastTime = new ThreadLocal<>();
    private ThreadLocal<Long> threadID = new ThreadLocal<>();
    private long nextID = 0L;

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

    private final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
    private final long SELF_CHECK_PERIOD = 10L;

    public void initializeZK(String zkAddress) {
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        this.leaderSelector = new LeaderSelector(client, PATH + "/leader", this);
        this.leaderSelector.autoRequeue();
        this.leaderSelector.start();
        this.timerExecutor.scheduleAtFixedRate( new Runnable() {
            @Override
            public void run() {
                while (!leaderExists()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LOGGER.warn("Unexpected thread interruption!");
                    }
                }
//                while (true) {
                    if (isLeader) {
                        return;
                    }
                    while (!tryGetInstanceID()) ;
                    //心跳，需要考虑网络不通畅时，别人抢占了自己的节点，需要重新获取

                    try {
                        if (isLeader)
                            return;
                        byte[] data = client.getData().forPath(PATH + "/instance/" + instanceId);
                        if (data == null || !new String(data).equals(ID)) {
                            while (!tryGetInstanceID()) ;
                        } else{
                            return;
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Exception caught:" + e.getCause());
                    }
//                }
            }
        },0L,SELF_CHECK_PERIOD,TimeUnit.SECONDS);
    }

    private boolean tryGetInstanceID() {
        try {
            this.ready = false;
            byte[] data = this.client.getData().forPath(PATH + "/next");
            String nextCounter = new String(data);
            this.instanceId = Integer.parseInt(nextCounter);
            if (this.client.checkExists().forPath(PATH + "/instance/" + this.instanceId) == null)
                this.client.create().withMode(CreateMode.EPHEMERAL).forPath(PATH + "/instance/" + this.instanceId, ID.getBytes());
            else
                return false;
            this.ready = true;
            return true;
        } catch (Exception e) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                LOGGER.warn("Unexpected thread interruption!");
            }
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

    @Override
    public long nextId(String prefixName) {
        while (!ready) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.warn("Unexpected thread interruption!");
            }
        }
        long time = System.currentTimeMillis();
        if (threadLastTime.get() == null) {
            threadLastTime.set(time);
        }
        if (threadInc.get() == null) {
            threadInc.set(0L);
        }
        if (threadID.get() == null) {
            threadID.set(getNextThreadID());
        }
        long a = threadInc.get();
        if ((a + 1L) >= maxIncrement) {
            if (threadLastTime.get() == time) {
                time = blockUntilNextMillis(time);
            }
            threadInc.set(0L);
        } else {
            threadInc.set(a + 1L);
        }
        threadLastTime.set(time);
        return ((time & timestampMask) << timestampShift) | (((threadID.get() % maxThreadId) << threadIdShift)) | (instanceId << instanceIdShift) | (clusterId << clusterIdShift) | a;
    }

    private synchronized Long getNextThreadID() {
        long i = nextID;
        nextID++;
        return i;
    }

    private long blockUntilNextMillis(long time) {
        while (System.currentTimeMillis() == time) ;
        return System.currentTimeMillis();
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
                            reloadChild();
                            client.setData().forPath(PATH + "/next", ("" + nextFree()).getBytes());
                            break;
                        }

                        case CHILD_REMOVED: {
                            LOGGER.debug("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            reloadChild();
                            client.setData().forPath(PATH + "/next", ("" + nextFree()).getBytes());
                            break;
                        }
                    }
                }
            };
            cache.getListenable().addListener(listener);
            cache.start();
            reloadChild();
            if (this.client.checkExists().forPath(PATH + "/next") == null)
                this.client.create().withMode(CreateMode.EPHEMERAL).forPath(PATH + "/next", ("" + nextFree()).getBytes());
            this.ready = true;
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            LOGGER.warn("Unexpected thread interruption!");
        } catch (Exception e) {
            LOGGER.warn("Exception caught:" + e.getMessage() + "Cause:" + e.getCause() + e.getStackTrace().toString());
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

    public static void main(String[] args) throws InterruptedException {
        System.out.println((1L << 38L) - 1L);
        System.out.println(new Date(System.currentTimeMillis() + (1L << 39L) - 1L));
    }
}
