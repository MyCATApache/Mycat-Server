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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.mycat.backend.PhysicalDBPool;
import io.mycat.cache.CacheService;
import io.mycat.net.*;
import io.mycat.route.MyCATSequnceProcessor;
import io.mycat.route.RouteService;
import io.mycat.server.MySQLFrontConnectionFactory;
import io.mycat.server.MySQLFrontConnectionHandler;
import io.mycat.server.classloader.DynaClassLoader;
import io.mycat.server.config.ConfigException;
import io.mycat.server.config.cluster.ClusterSync;
import io.mycat.server.config.loader.ConfigFactory;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.interceptor.SQLInterceptor;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.util.TimeUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
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
	private static final MycatServer INSTANCE = new MycatServer();
	private static final Logger LOGGER = LoggerFactory.getLogger("MycatServer");
	private final RouteService routerService;
	private final CacheService cacheService;
	private AsynchronousChannelGroup[] asyncChannelGroups;
	private volatile int channelIndex = 0;
	private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();
	private final DynaClassLoader catletClassLoader;
	private final SQLInterceptor sqlInterceptor;
	private final AtomicLong xaIDInc = new AtomicLong();

	public static final MycatServer getInstance() {
		return INSTANCE;
	}

	private final MycatConfig config;
	private final Timer timer;
	private final AtomicBoolean isOnline;
	private final long startupTime;
	private NamebleScheduledExecutor timerExecutor;
	private ListeningExecutorService listeningExecutorService;

	private ClusterSync clusterSync;

	private MycatServer() {
		this.config = new MycatConfig();
		this.timer = new Timer(NAME + "Timer", true);
		this.isOnline = new AtomicBoolean(true);
		cacheService = new CacheService();
		routerService = new RouteService(cacheService);
		try {
			sqlInterceptor = (SQLInterceptor) Class.forName(
					config.getSystem().getSqlInterceptor()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		catletClassLoader = new DynaClassLoader(SystemConfig.getHomePath()
				+ File.separator + "catlet", config.getSystem()
				.getCatletClassCheckSeconds());


		this.startupTime = TimeUtil.currentTimeMillis();
	}

	public DynaClassLoader getCatletClassLoader() {
		return catletClassLoader;
	}

	public MyCATSequnceProcessor getSequnceProcessor() {
		return sequnceProcessor;
	}

	public SQLInterceptor getSqlInterceptor() {
		return sqlInterceptor;
	}

	public String genXATXID() {
		long seq = this.xaIDInc.incrementAndGet();
		if (seq < 0) {
			synchronized (xaIDInc) {
				if (xaIDInc.get() < 0) {
					xaIDInc.set(0);
				}
				seq = xaIDInc.incrementAndGet();
			}
		}
		return "'Mycat." + this.getConfig().getSystem().getMycatNodeId() + "."
				+ seq + "'";
	}

	/**
	 * get next AsynchronousChannel ,first is exclude if multi
	 * AsynchronousChannelGroups
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

	public void startup() throws IOException {

		SystemConfig system = config.getSystem();
		int processorCount = system.getProcessors();
		
		// server startup
		LOGGER.info("===============================================");
		LOGGER.info(NAME + " is ready to startup ...");
		String inf = "Startup processors ...,total processors:"
				+ system.getProcessors() + ",aio thread pool size:"
				+ system.getProcessorExecutor()
				+ "    \r\n each process allocated socket buffer pool "
				+ " bytes ,buffer chunk size:"
				+ system.getProcessorBufferChunk()
				+ "  buffer pool's capacity(buferPool/bufferChunk) is:"
				+ system.getProcessorBufferPool()
				/ system.getProcessorBufferChunk();
		LOGGER.info(inf);
		LOGGER.info("sysconfig params:" + system.toString());

		int threadPoolSize = system.getProcessorExecutor();
		long processBuferPool = system.getProcessorBufferPool();
		int processBufferChunk = system.getProcessorBufferChunk();
		int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();

		// server startup
		LOGGER.info("===============================================");
		LOGGER.info(NAME + " is ready to startup ,network config:" + system);

		// message byte buffer pool
		BufferPool bufferPool = new BufferPool(processBuferPool,
				processBufferChunk, system.getFrontSocketSoRcvbuf(),
				socketBufferLocalPercent / processorCount);
		// Business Executor ，用来执行那些耗时的任务
		NameableExecutor businessExecutor = ExecutorUtil.create(
				"BusinessExecutor", threadPoolSize);
		// 定时器Executor，用来执行定时任务
		timerExecutor = ExecutorUtil.createSheduledExecute("Timer",
				system.getTimerExecutor());
		listeningExecutorService = MoreExecutors
				.listeningDecorator(businessExecutor);

		// create netsystem to store our network related objects
		NetSystem netSystem = new NetSystem(bufferPool, businessExecutor,
				timerExecutor);
		netSystem.setNetConfig(system);
		// Reactor pool
		NIOReactorPool reactorPool = new NIOReactorPool(
				BufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR", processorCount);
		NIOConnector connector = new NIOConnector(
				BufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", reactorPool);
		connector.start();
		netSystem.setConnector(connector);

		MySQLFrontConnectionFactory frontFactory = new MySQLFrontConnectionFactory(
				new MySQLFrontConnectionHandler());
		NIOAcceptor server = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX
				+ NAME + "Server", system.getBindIp(), system.getServerPort(),
				frontFactory, reactorPool);
		server.start();
		// server started
		LOGGER.info(server.getName() + " is started and listening on "
				+ server.getPort());
		
		// init datahost
		config.initDatasource();
		
		long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod();
		timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);
		timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());
		timer.schedule(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L,
				dataNodeIldeCheckPeriod);
		timer.schedule(dataNodeHeartbeat(), 0L,
				system.getDataNodeHeartbeatPeriod());
		if(system.isGlobalTableCheckSwitchOn())	// 全局表一致性检测是否开启
			timer.schedule(glableTableConsistencyCheck(), 0L, 
							system.getGlableTableCheckPeriod());
		timer.schedule(catletClassClear(), 30000);
	
	}

	private TimerTask catletClassClear() {
		return new TimerTask() {
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
	
	

	public RouteService getRouterService() {
		return routerService;
	}

	public CacheService getCacheService() {
		return cacheService;
	}

	public RouteService getRouterservice() {
		return routerService;
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

	// 系统时间定时更新任务
	private TimerTask updateTime() {
		return new TimerTask() {
			@Override
			public void run() {
				TimeUtil.update();
			}
		};
	}

	// 处理器定时检查任务
	private TimerTask processorCheck() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							NetSystem.getInstance().checkConnections();
						} catch (Exception e) {
							LOGGER.warn("checkBackendCons caught err:", e);
						}

					}
				});
			}
		};
	}

	// 数据节点定时连接空闲超时检查任务
	private TimerTask dataNodeConHeartBeatCheck(final long heartPeriod) {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.heartbeatCheck(heartPeriod);
						}
						Map<String, PhysicalDBPool> _nodes = config
								.getBackupDataHosts();
						if (_nodes != null) {
							for (PhysicalDBPool node : _nodes.values()) {
								node.heartbeatCheck(heartPeriod);
							}
						}
					}
				});
			}
		};
	}

	//  全局表一致性检查任务
	private TimerTask glableTableConsistencyCheck() {
		return new TimerTask() {
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
	
	// 数据节点定时心跳任务
	private TimerTask dataNodeHeartbeat() {
		return new TimerTask() {
			@Override
			public void run() {
				timerExecutor.execute(new Runnable() {
					@Override
					public void run() {
						Map<String, PhysicalDBPool> nodes = config
								.getDataHosts();
						for (PhysicalDBPool node : nodes.values()) {
							node.doHeartbeat();
						}
					}
				});
			}
		};
	}

	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}
	/**
	 * save cur datanode index to properties file
	 *
	 * @param dataNode
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {
		if(clusterSync==null){
			clusterSync = ConfigFactory.instanceCluster();
		}
		boolean isSwitch = clusterSync.switchDataSource(dataHost, curIndex);
		if(isSwitch){
			config.setHostIndex(dataHost, curIndex);
		}else {
			LOGGER.warn("can't switch dataHost"+dataHost +" to curIndex " + curIndex);
			throw new ConfigException("can't switch dataHost"+dataHost +" to curIndex " + curIndex);
		}
	}
}
