package io.mycat.route.sequence.handler;


import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.util.PropertiesUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
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

    private ThreadLocal<Long> threadInc = new ThreadLocal<>();
    private ThreadLocal<Long> threadLastTime = new ThreadLocal<>();
    private ThreadLocal<Long> threadID = new ThreadLocal<>();
    private long nextID = 0L;

    private final static String PATH = ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_BASE.getKey()
            + io.mycat.config.loader.zkprocess.comm.ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID)
            + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE.getKey();
    // private final static String PATH = "/mycat/sequence";
    private final static String INSTANCE_PATH = ZookeeperPath.ZK_SEPARATOR.getKey()
            + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_INSTANCE.getKey();
    private final static String LEADER_PATH = ZookeeperPath.ZK_SEPARATOR.getKey()
            + ZookeeperPath.FLOW_ZK_PATH_SEQUENCE_LEADER.getKey();
    private SystemConfig mycatConfig;
    private String ID;

    private int mark[];
    private volatile boolean isLeader = false;
    private volatile String slavePath;
    // 配置是否载入好
    private volatile boolean ready = false;

    private CuratorFramework client;

    private LeaderSelector leaderSelector;

    private final ScheduledExecutorService timerExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService leaderExecutor;
    private final long SELF_CHECK_PERIOD = 10L;

    public static DistributedSequenceHandler getInstance(SystemConfig systemConfig) {
        if (instance == null) {
            instance = new DistributedSequenceHandler(systemConfig);
        }
        return instance;
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public LeaderSelector getLeaderSelector() {
        return leaderSelector;
    }

    public long getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(long instanceId) {
        this.instanceId = instanceId;
    }

    public CuratorFramework getClient() {
        return client;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }

    public DistributedSequenceHandler(SystemConfig mycatConfig) {
        this.mycatConfig = mycatConfig;
        ID = mycatConfig.getBindIp() + mycatConfig.getServerPort();
    }

    public void load() {
        // load sequnce properties
        Properties props = PropertiesUtil.loadProps(SEQUENCE_DB_PROPS);
        if ("ZK".equals(props.getProperty("INSTANCEID"))) {
            initializeZK(ZkConfig.getInstance().getZkURL());
        } else {
            this.instanceId = Long.parseLong(props.getProperty("INSTANCEID"));
            this.ready = true;
        }
        this.clusterId = Long.valueOf(props.getProperty("CLUSTERID"));

    }

    public void initializeZK(String zkAddress) {
        this.client = CuratorFrameworkFactory.newClient(zkAddress, new ExponentialBackoffRetry(1000, 3));
        this.client.start();
        try {
            if (client.checkExists().forPath(PATH.concat(INSTANCE_PATH)) == null) {
                client.create().creatingParentContainersIfNeeded().forPath(PATH.concat(INSTANCE_PATH));
            }
        } catch (Exception e) {
            // do nothing
        }
        this.leaderSelector = new LeaderSelector(client, PATH.concat(LEADER_PATH), this);
        this.leaderSelector.autoRequeue();
        this.leaderSelector.start();
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    while (leaderSelector.getLeader() == null) {
                        Thread.currentThread().yield();
                    }
                    if (!leaderSelector.hasLeadership()) {
                        isLeader = false;
                        if (slavePath != null && client.checkExists().forPath(slavePath) != null) {
                            return;
                        }
                        slavePath = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                                .forPath(PATH.concat("/instance/node"), "ready".getBytes());
                        while ("ready".equals(new String(client.getData().forPath(slavePath)))) {
                            Thread.currentThread().yield();
                        }
                        instanceId = Long.parseLong(new String(client.getData().forPath(slavePath)));
                        ready = true;
                    }
                } catch (Exception e) {
                    LOGGER.warn("Caught exception while handling zk!", e);
                }
            }
        };
        timerExecutor.scheduleAtFixedRate(runnable, 1L, 10L, TimeUnit.SECONDS);
    }

    @Override
    public long nextId(String prefixName) {
        // System.out.println(instanceId);
        while (!ready) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                LOGGER.warn("Unexpected thread interruption!");
                Thread.currentThread().interrupt();
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
        return ((time & timestampMask) << timestampShift) | (((threadID.get() % maxThreadId) << threadIdShift))
                | (instanceId << instanceIdShift) | (clusterId << clusterIdShift) | a;
    }

    private synchronized Long getNextThreadID() {
        long i = nextID;
        nextID++;
        return i;
    }

    private long blockUntilNextMillis(long time) {
        while (System.currentTimeMillis() == time) {
        }
        return System.currentTimeMillis();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
            this.isLeader = false;
            leaderExecutor.shutdownNow();
            throw new CancelLeadershipException();
        }
    }

    @Override
    public void takeLeadership(final CuratorFramework curatorFramework) {
        this.isLeader = true;
        this.instanceId = 1;
        this.ready = true;
        this.mark = new int[(int) maxinstanceId];
        List<String> children = null;
        try {
            if (this.slavePath != null) {
                client.delete().forPath(slavePath);
            }
            if (client.checkExists().forPath(PATH.concat(INSTANCE_PATH)) != null) {
                children = client.getChildren().forPath(PATH.concat(INSTANCE_PATH));
            }
            if (children != null) {
                for (String child : children) {
                    String data = new String(
                            client.getData().forPath(PATH.concat(INSTANCE_PATH.concat("/").concat(child))));
                    if (!"ready".equals(data)) {
                        mark[Integer.parseInt(data)] = 1;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Caught exception while handling zk!", e);
        }

        leaderExecutor = Executors.newSingleThreadScheduledExecutor();
        leaderExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    while (!client.isStarted()) {
                        Thread.currentThread().yield();
                    }
                    List<String> children = client.getChildren().forPath(PATH.concat(INSTANCE_PATH));
                    int mark2[] = new int[(int) maxinstanceId];
                    for (String child : children) {
                        String data = new String(client.getData().forPath(PATH.concat("/instance/" + child)));
                        if ("ready".equals(data)) {
                            int i = nextFree();
                            client.setData().forPath(PATH.concat(INSTANCE_PATH.concat("/").concat(child)),
                                    ("" + i).getBytes());
                            mark2[i] = 1;
                        } else {
                            mark2[Integer.parseInt(data)] = 1;
                        }
                    }
                    mark = mark2;
                } catch (Exception e) {
                    LOGGER.warn("Caught exception while handling zk!", e);
                }
            }
        }, 0L, 3L, TimeUnit.SECONDS);
        while (true) {
            Thread.currentThread().yield();
        }
    }

    private int nextFree() {
        for (int i = 0; i < mark.length; i++) {
            if (i == 1) {
                continue;
            }
            if (mark[i] != 1) {
                mark[i] = 1;
                return i;
            }
        }
        return -1;
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(this.leaderSelector);
        CloseableUtils.closeQuietly(this.client);
    }
}
