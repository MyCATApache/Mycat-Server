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
package io.mycat;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.heartbeat.zkprocess.MycatLeaderLatch;
import io.mycat.backend.mysql.nio.handler.MultiNodeCoordinator;
import io.mycat.backend.mysql.xa.CoordinatorLogEntry;
import io.mycat.backend.mysql.xa.ParticipantLogEntry;
import io.mycat.backend.mysql.xa.TxState;
import io.mycat.backend.mysql.xa.XARollbackCallback;
import io.mycat.backend.mysql.xa.recovery.Repository;
import io.mycat.backend.mysql.xa.recovery.impl.FileSystemRepository;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.DirectByteBufferPool;
import io.mycat.buffer.NettyBufferPool;
import io.mycat.cache.CacheService;
import io.mycat.config.MycatConfig;
import io.mycat.config.classloader.DynaClassLoader;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.table.structure.MySQLTableStructureDetector;
import io.mycat.manager.ManagerConnectionFactory;
import io.mycat.memory.MyCatMemory;
import io.mycat.net.*;
import io.mycat.route.MyCATSequnceProcessor;
import io.mycat.route.RouteService;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.route.sequence.handler.SequenceHandler;
import io.mycat.server.ServerConnectionFactory;
import io.mycat.server.interceptor.SQLInterceptor;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.statistic.SQLRecorder;
import io.mycat.statistic.stat.SqlResultSizeRecorder;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;
import io.mycat.util.ExecutorUtil;
import io.mycat.util.NameableExecutor;
import io.mycat.util.TimeUtil;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public class MycatServer {

    public static final String NAME = "MyCat";
    private static final long LOG_WATCH_DELAY = 60000L;
    private static final long TIME_UPDATE_PERIOD = 20L;
    private static final long DEFAULT_SQL_STAT_RECYCLE_PERIOD = 5 * 1000L;
    private static final long DEFAULT_OLD_CONNECTION_CLEAR_PERIOD = 5 * 1000L;

    private static final MycatServer INSTANCE = new MycatServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("MycatServer");
    private static final Repository fileRepository = new FileSystemRepository();
    private final RouteService routerService;
    private final CacheService cacheService;
    private Properties dnIndexProperties;

    //AIO连接群组 -- 异步通道组
    private AsynchronousChannelGroup[] asyncChannelGroups;
    private volatile int channelIndex = 0;

    //全局序列号
//  private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();
    private final DynaClassLoader catletClassLoader;
    private final SQLInterceptor sqlInterceptor;
    private volatile int nextProcessor;

    // System Buffer Pool Instance 系统缓冲池实例
    private BufferPool bufferPool;
    private boolean aio = false;

    //XA事务全局ID生成
    private final AtomicLong xaIDInc = new AtomicLong();
    //sequence处理对象
    private SequenceHandler sequenceHandler;

    /**
     * Mycat 内存管理类
     */
    private MyCatMemory myCatMemory = null;

    public static final MycatServer getInstance() {
        return INSTANCE;
    }

    private final MycatConfig config;
    private final ScheduledExecutorService scheduler;
    private final ScheduledExecutorService heartbeatScheduler;
    private final SQLRecorder sqlRecorder;
    private final AtomicBoolean isOnline;
    private final long startupTime;
    private NIOProcessor[] processors;
    private SocketConnector connector;
    // 业务执行器
    private NameableExecutor businessExecutor;
    // 序列执行器
    private NameableExecutor sequenceExecutor;
    // 计时执行器
    private NameableExecutor timerExecutor;
    // 监听执行器服务
    private ListeningExecutorService listeningExecutorService;
    private InterProcessMutex dnindexLock;
    private long totalNetWorkBufferSize = 0;

    private volatile MycatLeaderLatch leaderLatch;

    private final AtomicBoolean startup=new AtomicBoolean(false);
    private MycatServer() {

        //读取文件配置
        this.config = new MycatConfig();

        //定时线程池，单线程线程池
        scheduler = Executors.newSingleThreadScheduledExecutor();

        //心跳调度独立出来，避免被其他任务影响
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

        //SQL记录器
        this.sqlRecorder = new SQLRecorder(config.getSystem().getSqlRecordCount());

        /**
         * 是否在线，MyCat manager中有命令控制
         * | offline | Change MyCat status to OFF |
         * | online | Change MyCat status to ON |
         */
        this.isOnline = new AtomicBoolean(true);

        //缓存服务初始化
        cacheService = new CacheService();

        //路由计算初始化
        routerService = new RouteService(cacheService);

        // 从属性加载datanode活动索引
        dnIndexProperties = loadDnIndexProps();
        try {
            //SQL拦截器
            sqlInterceptor = (SQLInterceptor) Class.forName(
                    config.getSystem().getSqlInterceptor()).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // catlet加载器
        catletClassLoader = new DynaClassLoader(SystemConfig.getHomePath()
                + File.separator + "catlet", config.getSystem().getCatletClassCheckSeconds());

        // 记录启动时间
        this.startupTime = TimeUtil.currentTimeMillis();
        if(isUseZkSwitch()) {
            String path = ZKUtils.getZKBasePath()+"lock/dnindex.lock";
            dnindexLock = new InterProcessMutex(ZKUtils.getConnection(), path);
        }

    }

    public AtomicBoolean getStartup() {
        return startup;
    }

    public long getTotalNetWorkBufferSize() {
        return totalNetWorkBufferSize;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }

    public DynaClassLoader getCatletClassLoader() {
        return catletClassLoader;
    }

    public MyCATSequnceProcessor getSequnceProcessor() {
        return MyCATSequnceProcessor.getInstance();
    }

    public SQLInterceptor getSqlInterceptor() {
        return sqlInterceptor;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public String genXATXID() {
        long seq = this.xaIDInc.incrementAndGet();
        if (seq < 0) {
            synchronized (xaIDInc) {
                if ( xaIDInc.get() < 0 ) {
                    xaIDInc.set(0);
                }
                seq = xaIDInc.incrementAndGet();
            }
        }
        return "'Mycat." + this.getConfig().getSystem().getMycatNodeId() + "." + seq + "'";
    }

    public String getXATXIDGLOBAL(){
        return "'" + getUUID() + "'";
    }

    public static String getUUID(){
        String s = UUID.randomUUID().toString();
        //去掉“-”符号
        return s.substring(0,8)+s.substring(9,13)+s.substring(14,18)+s.substring(19,23)+s.substring(24);
    }

    public MyCatMemory getMyCatMemory() {
        return myCatMemory;
    }

    /**
     * get next AsynchronousChannel ,first is exclude if multi
     * AsynchronousChannelGroups
     *
     * 获取下一个AsynchronousChannel，首先排除多个AsynchronousChannelGroups
     *
     * @return
     */
    public AsynchronousChannelGroup getNextAsyncChannelGroup() {
        if (asyncChannelGroups.length == 1) {
            return asyncChannelGroups[0];
        } else {
            int index = (++channelIndex) % asyncChannelGroups.length;
            if (index == 0) {
                ++channelIndex;
                return asyncChannelGroups[1];
            } else {
                return asyncChannelGroups[index];
            }

        }
    }

    public MycatConfig getConfig() {
        return config;
    }

    /**
     * 开启前处理
     */
    public void beforeStart() {
        String home = SystemConfig.getHomePath();
        //ZkConfig.instance().initZk();
    }

    /**
     * 启动
     * @throws IOException
     */
    public void startup() throws IOException {

        SystemConfig system = config.getSystem();
        int processorCount = system.getProcessors();

        // 初始化路由策略工厂
        RouteStrategyFactory.init();

        // 服务器启动
        LOGGER.info(NAME + " is ready to startup ...");
        String inf = "Startup processors ...,total processors:"
                + system.getProcessors() + ",aio thread pool size:"
                + system.getProcessorExecutor()
                + "    \r\n each process allocated socket buffer pool "
                + " bytes ,a page size:"
                + system.getBufferPoolPageSize()
                + "  a page's chunk number(PageSize/ChunkSize) is:"
                + (system.getBufferPoolPageSize()
                /system.getBufferPoolChunkSize())
                + "  buffer page's number is:"
                + system.getBufferPoolPageNumber();
        LOGGER.info(inf);
        LOGGER.info("sysconfig params:" + system.toString());

        // 启动管理器
        ManagerConnectionFactory mf = new ManagerConnectionFactory();
        ServerConnectionFactory sf = new ServerConnectionFactory();
        SocketAcceptor manager = null;
        SocketAcceptor server = null;
        aio = (system.getUsingAIO() == 1);

        // 启动处理器
        int threadPoolSize = system.getProcessorExecutor();
        processors = new NIOProcessor[processorCount];
        // a page size 一个页面大小
        int bufferPoolPageSize = system.getBufferPoolPageSize();
        // total page number 总页数
        short bufferPoolPageNumber = system.getBufferPoolPageNumber();
        // minimum allocation unit 最小分配单位
        short bufferPoolChunkSize = system.getBufferPoolChunkSize();

        int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();
        int bufferPoolType = system.getProcessorBufferPoolType();

        switch (bufferPoolType){
            case 0:
                bufferPool = new DirectByteBufferPool(bufferPoolPageSize,bufferPoolChunkSize,
                        bufferPoolPageNumber,system.getFrontSocketSoRcvbuf());


                totalNetWorkBufferSize = bufferPoolPageSize*bufferPoolPageNumber;
                break;
            case 1:
                /**
                 * todo 对应权威指南修改：
                 *
                 * bytebufferarena由6个bytebufferlist组成，这六个list有减少内存碎片的机制
                 * 每个bytebufferlist由多个bytebufferchunk组成，每个list也有减少内存碎片的机制
                 * 每个bytebufferchunk由多个page组成，平衡二叉树管理内存使用状态，计算灵活
                 * 设置的pagesize对应bytebufferarena里面的每个bytebufferlist的每个bytebufferchunk的buffer长度
                 * bufferPoolChunkSize对应每个bytebufferchunk的每个page的长度
                 * bufferPoolPageNumber对应每个bytebufferlist有多少个bytebufferchunk
                 */
                totalNetWorkBufferSize = 6*bufferPoolPageSize * bufferPoolPageNumber;
                break;
            case 2:
                bufferPool = new NettyBufferPool(bufferPoolChunkSize);
                LOGGER.info("Use Netty Buffer Pool");
                break;
            default:
                bufferPool = new DirectByteBufferPool(bufferPoolPageSize,bufferPoolChunkSize,
                        bufferPoolPageNumber,system.getFrontSocketSoRcvbuf());
                totalNetWorkBufferSize = bufferPoolPageSize*bufferPoolPageNumber;
        }

        /**
         * Off Heap For Merge/Order/Group/Limit 初始化
         */
        if(system.getUseOffHeapForMerge() == 1){
            try {
                myCatMemory = new MyCatMemory(system,totalNetWorkBufferSize);
            } catch (NoSuchFieldException e) {
                LOGGER .error("NoSuchFieldException",e);
            } catch (IllegalAccessException e) {
                LOGGER.error("Error",e);
            }
        }
        // 业务执行器
        businessExecutor = ExecutorUtil.create("BusinessExecutor", threadPoolSize);
        // 序列执行器
        sequenceExecutor = ExecutorUtil.create("SequenceExecutor", threadPoolSize);
        // 计时执行器
        timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
        // 监听执行器服务
        listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);

        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor("Processor" + i, bufferPool, businessExecutor);
        }

        if (aio) {
            LOGGER.info("using aio network handler ");
            asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
            // 创建连接器
            connector = new AIOConnector();
            for (int i = 0; i < processors.length; i++) {
                asyncChannelGroups[i] = AsynchronousChannelGroup.withFixedThreadPool(processorCount,
                        new ThreadFactory() {
                            private int inx = 1;
                            @Override
                            public Thread newThread(Runnable r) {
                                Thread th = new Thread(r);
                                //TODO
                                th.setName(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "AIO" + (inx++));
                                LOGGER.info("created new AIO thread "+ th.getName());
                                return th;
                            }
                        }
                );
            }
            // 创建管理器的接收器
            manager = new AIOAcceptor(NAME + "Manager", system.getBindIp(),
                    system.getManagerPort(), mf, this.asyncChannelGroups[0]);

            // 创建服务器的接收器
            server = new AIOAcceptor(NAME + "Server", system.getBindIp(),
                    system.getServerPort(), sf, this.asyncChannelGroups[0]);

        } else {
            LOGGER.info("using nio network handler ");

            NIOReactorPool reactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR",
                    processors.length);
            // 创建连接器
            connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", reactorPool);
            ((NIOConnector) connector).start();
            // 创建管理器的接收器
            manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME
                    + "Manager", system.getBindIp(), system.getManagerPort(), mf, reactorPool);

            // 创建服务器的接收器
            server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME
                    + "Server", system.getBindIp(), system.getServerPort(), sf, reactorPool);
        }
        // manager start 管理器开始监听
        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());

        // server started 服务器开始监听
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());

        LOGGER.info("===============================================");

        // init datahost 初始化节点主机
        Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
        LOGGER.info("Initialize dataHost ...");
        for (PhysicalDBPool node : dataHosts.values()) {
            String index = dnIndexProperties.getProperty(node.getHostName(),"0");
            if (!"0".equals(index)) {
                LOGGER.info("init datahost: " + node.getHostName() + "  to use datasource index:" + index);
            }
            node.init(Integer.parseInt(index)); // 初始化节点主机
            node.startHeartbeat(); // 开始心跳
        }

        long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod(); // 分片节点空闲检查周期

        // 设置周期任务
        heartbeatScheduler.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD,TimeUnit.MILLISECONDS);
        heartbeatScheduler.scheduleAtFixedRate(processorCheck(), 0L, system.getProcessorCheckPeriod(),TimeUnit.MILLISECONDS);
        heartbeatScheduler.scheduleAtFixedRate(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L, dataNodeIldeCheckPeriod,TimeUnit.MILLISECONDS);
        heartbeatScheduler.scheduleAtFixedRate(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod(),TimeUnit.MILLISECONDS);
        heartbeatScheduler.scheduleAtFixedRate(dataSourceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);

        scheduler.schedule(catletClassClear(), 30000,TimeUnit.MILLISECONDS);

        if(system.getCheckTableConsistency()==1) { // 不同分片表结构一致性检测是否开启
            scheduler.scheduleAtFixedRate(tableStructureCheck(), 0L, system.getCheckTableConsistencyPeriod(), TimeUnit.MILLISECONDS);
        }

        if(system.getUseSqlStat()==1) { // SQL统计是否开启
            scheduler.scheduleAtFixedRate(recycleSqlStat(), 0L, DEFAULT_SQL_STAT_RECYCLE_PERIOD, TimeUnit.MILLISECONDS);
        }

        if(system.getUseGlobleTableCheck() == 1){   // 全局表一致性检测是否开启
//          scheduler.scheduleAtFixedRate(glableTableConsistencyCheck(), 0L, system.getGlableTableCheckPeriod(), TimeUnit.MILLISECONDS);
        }

        //定期清理结果集排行榜，控制拒绝策略
        scheduler.scheduleAtFixedRate(resultSetMapClear(),0L,  system.getClearBigSqLResultSetMapMs(),TimeUnit.MILLISECONDS);

//        new Thread(tableStructureCheck()).start();

        //XA Init recovery Log XA初始化回复日志
        LOGGER.info("===============================================");
        LOGGER.info("Perform XA recovery log ...");
        performXARecoveryLog(); // 检查并回滚数据

        if(isUseZkSwitch()) {
            // 首次启动如果发现zk上dnindex为空，则将本地初始化上zk
            initZkDnindex();

            leaderLatch = new  MycatLeaderLatch( "heartbeat/leader" );
            try {
                leaderLatch.start();
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
                e.printStackTrace();
            }
        }
        initRuleData();

        startup.set(true);
    }

    /**
     * 初始化路由数据 通过zk同步
     */
    public void initRuleData() {
        if(!isUseZk())  return;
        // InterProcessMutex 进程间互斥类
        InterProcessMutex ruleDataLock = null;
        try {
            File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "ruledata");
            if (!file.exists()) {
                file.mkdir();
            }
            String path = ZKUtils.getZKBasePath() + "lock/ruledata.lock";
            ruleDataLock = new InterProcessMutex(ZKUtils.getConnection(), path);
            ruleDataLock.acquire(30, TimeUnit.SECONDS);
            File[]  childFiles= file.listFiles();
            if(childFiles!=null&&childFiles.length>0) {
                String basePath = ZKUtils.getZKBasePath() + "ruledata/";
                for (File childFile : childFiles) {
                    CuratorFramework zk = ZKUtils.getConnection();
                    if (zk.checkExists().forPath(basePath + childFile.getName()) == null) {
                        zk.create().creatingParentsIfNeeded().forPath(basePath + childFile.getName(), Files.toByteArray(childFile));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(ruleDataLock!=null)
                    ruleDataLock.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 初始化数据节点配置 通过zk同步
     */
    private void initZkDnindex() {
        try {
            File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
            dnindexLock.acquire(30, TimeUnit.SECONDS);
            String path = ZKUtils.getZKBasePath() + "bindata/dnindex.properties";
            CuratorFramework zk = ZKUtils.getConnection();
            if (zk.checkExists().forPath(path) == null) {
                zk.create().creatingParentsIfNeeded().forPath(path, Files.toByteArray(file));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                dnindexLock.release();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 重新加载数据节点配置
     */
    public void reloadDnIndex() {
        if(MycatServer.getInstance().getProcessors()==null) return;
        // load datanode active index from properties
        dnIndexProperties = loadDnIndexProps();
        // init datahost
        Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
        LOGGER.info("reInitialize dataHost ...");
        for (PhysicalDBPool node : dataHosts.values()) {
            String index = dnIndexProperties.getProperty(node.getHostName(),"0");
            if (!"0".equals(index)) {
                LOGGER.info("reinit datahost: " + node.getHostName() + "  to use datasource index:" + index);
            }
            node.switchSource(Integer.parseInt(index),true,"reload dnindex");
        }
    }

    /**
     * 清除没用的动态类 定时调用
     * @return
     */
    private Runnable catletClassClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    catletClassLoader.clearUnUsedClass();
                } catch (Exception e) {
                    LOGGER.warn("catletClassClear err " + e);
                }
            }
        };
    }

    /**
     * 清理 reload @@config_all 后，老的 connection 连接
     * 定时调用
     * @return
     */
    private Runnable dataSourceOldConsClear() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        long sqlTimeout = MycatServer.getInstance().getConfig().getSystem().getSqlExecuteTimeout() * 1000L;

                        //根据 lastTime 确认事务的执行， 超过 sqlExecuteTimeout 阀值 close connection
                        long currentTime = TimeUtil.currentTimeMillis();
                        Iterator<BackendConnection> iter = NIOProcessor.backends_old.iterator();
                        while( iter.hasNext() ) {
                            BackendConnection con = iter.next();
                            long lastTime = con.getLastTime();
                            if ( currentTime - lastTime > sqlTimeout ) {
                                con.close("clear old backend connection ...");
                                iter.remove();
                            }
                        }
                    }
                });
            }
        };
    }

    /**
     * 在bufferpool使用率大于使用率阈值时不清理
     * 在bufferpool使用率小于使用率阈值时清理大结果集清单内容
     * 定时调用
     */
    private Runnable resultSetMapClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    BufferPool bufferPool=getBufferPool();
                    long bufferSize=bufferPool.size();
                    long bufferCapacity=bufferPool.capacity();
                    long bufferUsagePercent=(bufferCapacity-bufferSize)*100/bufferCapacity;
                    if(bufferUsagePercent<config.getSystem().getBufferUsagePercent()){
                        Map<String, UserStat> map =UserStatAnalyzer.getInstance().getUserStatMap();
                        Set<String> userSet=config.getUsers().keySet();
                        for (String user : userSet) {
                            UserStat userStat = map.get(user);
                            if(userStat!=null){
                                SqlResultSizeRecorder recorder=userStat.getSqlResultSizeRecorder();
                                //System.out.println(recorder.getSqlResultSet().size());
                                recorder.clearSqlResultSet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("resultSetMapClear err " + e);
                }
            }
        };
    }

    /**
     * 从属性加载datanode活动索引
     * @return
     */
    private Properties loadDnIndexProps() {
        Properties prop = new Properties();
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
        if (!file.exists()) {
            return prop;
        }
        FileInputStream filein = null;
        try {
            filein = new FileInputStream(file);
            prop.load(filein);
        } catch (Exception e) {
            LOGGER.warn("load DataNodeIndex err:" + e);
        } finally {
            if (filein != null) {
                try {
                    filein.close();
                } catch (IOException e) {
                }
            }
        }
        return prop;
    }


    public synchronized boolean saveDataHostIndexToZk(String dataHost, int curIndex) {
        boolean result = false;
        try {

            try {
                dnindexLock.acquire(30,TimeUnit.SECONDS)   ;
                String path = ZKUtils.getZKBasePath() + "bindata/dnindex.properties";

                Map<String,String> propertyMap = new HashMap<>();
                propertyMap.put(dataHost, String.valueOf(curIndex));
                result = ZKUtils.writeProperty( path, propertyMap);
            } finally {
                dnindexLock.release();
            }
        } catch (Exception e) {
            LOGGER.warn("saveDataHostIndexToZk err:", e);
        }
        return result;
    }

    /**
     * save cur datanode index to properties file
     * 将cur datanode索引保存到属性文件
     *
     * @param
     * @param curIndex
     */
    public synchronized void saveDataHostIndex(String dataHost, int curIndex) {
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
        FileOutputStream fileOut = null;
        try {
            String oldIndex = dnIndexProperties.getProperty(dataHost);
            String newIndex = String.valueOf(curIndex);
            if (newIndex.equals(oldIndex)) {
                return;
            }

            dnIndexProperties.setProperty(dataHost, newIndex);
            LOGGER.info("save DataHost index  " + dataHost + " cur index " + curIndex);

            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            fileOut = new FileOutputStream(file);
            dnIndexProperties.store(fileOut, "update");

//          if(isUseZkSwitch()) {
//              // save to  zk
//              try {
//                  dnindexLock.acquire(30,TimeUnit.SECONDS)   ;
//                  String path = ZKUtils.getZKBasePath() + "bindata/dnindex.properties";
//                  CuratorFramework zk = ZKUtils.getConnection();
//                  if(zk.checkExists().forPath(path)==null) {
//                      zk.create().creatingParentsIfNeeded().forPath(path, Files.toByteArray(file));
//                  } else{
//                      byte[] data=    zk.getData().forPath(path);
//                      ByteArrayOutputStream out=new ByteArrayOutputStream();
//                      Properties properties=new Properties();
//                      properties.load(new ByteArrayInputStream(data));
//                       if(!String.valueOf(curIndex).equals(properties.getProperty(dataHost))) {
//                           properties.setProperty(dataHost, String.valueOf(curIndex));
//                           properties.store(out, "update");
//                           zk.setData().forPath(path, out.toByteArray());
//                       }
//                  }
//
//              }finally {
//               dnindexLock.release();
//              }
//          }
        } catch (Exception e) {
            LOGGER.warn("saveDataNodeIndex err:", e);
        } finally {
            if (fileOut != null) {
                try {
                    fileOut.close();
                } catch (IOException e) {
                }
            }
        }

    }

    /**
     * 是否使用zk
     * @return
     */
    private boolean isUseZk() {
        String loadZk=ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_FLAG);
        return "true".equalsIgnoreCase(loadZk)   ;
    }

    /**
     * 是否使用zk切换
     * @return
     */
    public boolean isUseZkSwitch() {
        MycatConfig mycatConfig = config;
        boolean isUseZkSwitch = mycatConfig.getSystem().isUseZKSwitch();
        String loadZk = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_FLAG);
        return (isUseZkSwitch&&"true".equalsIgnoreCase(loadZk));
    }

    public RouteService getRouterService() {
        return routerService;
    }

    public CacheService getCacheService() {
        return cacheService;
    }

    public NameableExecutor getBusinessExecutor() {
        return businessExecutor;
    }

    public RouteService getRouterservice() {
        return routerService;
    }

    /**
     * 获取下一个处理器
     * @return
     */
    public NIOProcessor nextProcessor() {
        int i = ++nextProcessor;
        if (i >= processors.length) {
            i = nextProcessor = 0;
        }
        return processors[i];
    }

    public NIOProcessor[] getProcessors() {
        return processors;
    }

    public SocketConnector getConnector() {
        return connector;
    }

    public SQLRecorder getSqlRecorder() {
        return sqlRecorder;
    }

    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    /**
     * 系统时间定时更新任务
     * 定时任务
     * @return
     */
    private Runnable updateTime() {
        return new Runnable() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    /**
     * 处理器定时检查任务 回收资源
     * 定时任务
     * @return
     */
    private Runnable processorCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                // 回收后端部分资源
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkBackendCons caught err:" + e);
                        }
                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                // 回收前端部分资源
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkFrontCons caught err:" + e);
                        }
                    }
                });
            }
        };
    }

    /**
     * 数据节点定时连接空闲超时检查任务
     * 定时任务
     * @param heartPeriod
     * @return
     */
    private Runnable dataNodeConHeartBeatCheck(final long heartPeriod) {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            //后端物理连接心跳检查
                            node.heartbeatCheck(heartPeriod);
                        }
                        /*
                        Map<String, PhysicalDBPool> _nodes = config.getBackupDataHosts();
                        if (_nodes != null) {
                            for (PhysicalDBPool node : _nodes.values()) {
                                node.heartbeatCheck(heartPeriod);
                            }
                        }*/
                    }
                });
            }
        };
    }

    /**
     * 数据节点定时心跳任务
     * @return
     */
    private Runnable dataNodeHeartbeat() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Map<String, PhysicalDBPool> nodes = config.getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            // 执行心跳
                            node.doHeartbeat();
                        }
                    }
                });
            }
        };
    }

    /**
     * 定时清理保存SqlStat中的数据
     * @return
     */
    private Runnable recycleSqlStat(){
        return new Runnable() {
            @Override
            public void run() {
                Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
                for (UserStat userStat : statMap.values()) {
                    userStat.getSqlLastStat().recycle();
                    userStat.getSqlRecorder().recycle();
                    userStat.getSqlHigh().recycle();
                    userStat.getSqlLargeRowStat().recycle();
                }
            }
        };
    }

    /**
     * 定时检查不同分片表结构一致性
     * @return
     */
    private Runnable tableStructureCheck(){
        return new MySQLTableStructureDetector();
    }

    /**
     * 全局表一致性检查任务
     * @return
     */
    private Runnable glableTableConsistencyCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        GlobalTableUtil.consistencyCheck();
                    }
                });
            }
        };
    }


    /**
     * XA恢复日志检查 回滚数据
     */
    private void performXARecoveryLog() {
        //获取恢复日志
        CoordinatorLogEntry[] coordinatorLogEntries = getCoordinatorLogEntries();

        for(int i=0; i<coordinatorLogEntries.length; i++){
            CoordinatorLogEntry coordinatorLogEntry = coordinatorLogEntries[i];
            boolean needRollback = false;// 是否需要回滚
            for(int j=0; j<coordinatorLogEntry.participants.length; j++) {
                ParticipantLogEntry participantLogEntry = coordinatorLogEntry.participants[j];
                if (participantLogEntry.txState == TxState.TX_PREPARED_STATE){
                    needRollback = true;
                    break;
                }
            }
            if(needRollback){// 需要回滚
                for(int j=0; j<coordinatorLogEntry.participants.length; j++){
                    ParticipantLogEntry participantLogEntry = coordinatorLogEntry.participants[j];
                    //XA rollback
                    String xacmd = "XA ROLLBACK "+ coordinatorLogEntry.id +';';
                    OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler( new String[0], new XARollbackCallback());
                    outloop:
                    for (SchemaConfig schema : MycatServer.getInstance().getConfig().getSchemas().values()) { // 数据库
                        for (TableConfig table : schema.getTables().values()) { // 数据表
                            for (String dataNode : table.getDataNodes()) { // 数据节点
                                PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(dataNode); // 物理数据库节点
                                if (dn.getDbPool().getSource().getConfig().getIp().equals(participantLogEntry.uri)
                                        && dn.getDatabase().equals(participantLogEntry.resourceName)) {
                                    //XA STATE ROLLBACK
                                    participantLogEntry.txState = TxState.TX_ROLLBACKED_STATE;
                                    SQLJob sqlJob = new SQLJob(xacmd, dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
                                    sqlJob.run();
                                    LOGGER.debug(String.format("[XA ROLLBACK] [%s] Host:[%s] schema:[%s]", xacmd, dn.getName(), dn.getDatabase()));
                                    break outloop;
                                }
                            }
                        }
                    }
                }
            }
        }

        //init into in memory cached 初始化进入内存缓存
        for(int i=0;i<coordinatorLogEntries.length;i++){
            MultiNodeCoordinator.inMemoryRepository.put(coordinatorLogEntries[i].id,coordinatorLogEntries[i]);
        }
        //discard the recovery log 丢弃恢复日志
        MultiNodeCoordinator.fileRepository.writeCheckpoint(MultiNodeCoordinator.inMemoryRepository.getAllCoordinatorLogEntries());
    }

    /**
     * 将集合转换为数组
     * @return
     */
    private CoordinatorLogEntry[] getCoordinatorLogEntries(){
        // 从文件库中获取数据
        Collection<CoordinatorLogEntry> allCoordinatorLogEntries = fileRepository.getAllCoordinatorLogEntries();
        if(allCoordinatorLogEntries == null || allCoordinatorLogEntries.size()==0){
            return new CoordinatorLogEntry[0];
        }
        return allCoordinatorLogEntries.toArray(new CoordinatorLogEntry[allCoordinatorLogEntries.size()]);
    }

    public NameableExecutor getSequenceExecutor() {
        return sequenceExecutor;
    }

    //huangyiming add
    public DirectByteBufferPool getDirectByteBufferPool() {
        return (DirectByteBufferPool)bufferPool;
    }

    public boolean isAIO() {
        return aio;
    }

    public ListeningExecutorService getListeningExecutorService() {
        return listeningExecutorService;
    }
    public ScheduledExecutorService getHeartbeatScheduler() {
        return heartbeatScheduler;
    }

    public MycatLeaderLatch getLeaderLatch() {
        return leaderLatch;
    }
    public static void main(String[] args) throws Exception {
        String path = ZKUtils.getZKBasePath() + "bindata";
        CuratorFramework zk = ZKUtils.getConnection();
        if(zk.checkExists().forPath(path)==null);

        byte[] data = zk.getData().forPath(path);
        System.out.println(data.length);
    }

}
