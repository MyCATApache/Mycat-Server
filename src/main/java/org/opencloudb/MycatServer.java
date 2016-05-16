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
package org.opencloudb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.log4j.Logger;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.buffer.BufferPool;
import org.opencloudb.cache.CacheService;
import org.opencloudb.classloader.DynaClassLoader;
import org.opencloudb.config.ZkConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.manager.ManagerConnectionFactory;
import org.opencloudb.net.AIOAcceptor;
import org.opencloudb.net.AIOConnector;
import org.opencloudb.net.NIOAcceptor;
import org.opencloudb.net.NIOConnector;
import org.opencloudb.net.NIOProcessor;
import org.opencloudb.net.NIOReactorPool;
import org.opencloudb.net.SocketAcceptor;
import org.opencloudb.net.SocketConnector;
import org.opencloudb.route.MyCATSequnceProcessor;
import org.opencloudb.route.RouteService;
import org.opencloudb.server.ServerConnectionFactory;
import org.opencloudb.statistic.SQLRecorder;
import org.opencloudb.util.ExecutorUtil;
import org.opencloudb.util.NameableExecutor;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class MycatServer {
	public static final String NAME = "MyCat";
	private static final long LOG_WATCH_DELAY = 60000L;
	private static final long TIME_UPDATE_PERIOD = 20L;
	private static final MycatServer INSTANCE = new MycatServer();
	private static final Logger LOGGER = Logger.getLogger("MycatServer");
	private final RouteService routerService;
	private final CacheService cacheService;
	private Properties dnIndexProperties;
	private AsynchronousChannelGroup[] asyncChannelGroups;
	private volatile int channelIndex = 0;
	private final MyCATSequnceProcessor sequnceProcessor = new MyCATSequnceProcessor();
	private final DynaClassLoader catletClassLoader;
	private final SQLInterceptor sqlInterceptor;
	private volatile int nextProcessor;
	private BufferPool bufferPool;
	private boolean aio = false;
	private final AtomicLong xaIDInc = new AtomicLong();

	public static final MycatServer getInstance() {
		return INSTANCE;
	}

	private final MycatConfig config;
	private final Timer timer;
	private final SQLRecorder sqlRecorder;
	private final AtomicBoolean isOnline;
	private final long startupTime;
	private NIOProcessor[] processors;
	private SocketConnector connector;
	private NameableExecutor businessExecutor;
	private NameableExecutor timerExecutor;
	private ListeningExecutorService listeningExecutorService;

	public MycatServer() {
		//载入配置文件
		this.config = new MycatConfig();
		this.timer = new Timer(NAME + "Timer", true);
		this.sqlRecorder = new SQLRecorder(config.getSystem()
				.getSqlRecordCount());
		this.isOnline = new AtomicBoolean(true);
		cacheService = new CacheService();
		routerService = new RouteService(cacheService);
		// load datanode active index from properties
		dnIndexProperties = loadDnIndexProps();
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
				+ seq+"'";
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

	public void beforeStart() {
		String home = SystemConfig.getHomePath();
		Log4jInitializer.configureAndWatch(home + "/conf/log4j.xml", LOG_WATCH_DELAY);
		
		//ZkConfig.instance().initZk();
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

		// startup manager
		ManagerConnectionFactory mf = new ManagerConnectionFactory();
		ServerConnectionFactory sf = new ServerConnectionFactory();
		SocketAcceptor manager = null;
		SocketAcceptor server = null;
		aio = (system.getUsingAIO() == 1);

		// startup processors
		int threadPoolSize = system.getProcessorExecutor();
		processors = new NIOProcessor[processorCount];
		long processBuferPool = system.getProcessorBufferPool();
		int processBufferChunk = system.getProcessorBufferChunk();
		int socketBufferLocalPercent = system.getProcessorBufferLocalPercent();
		bufferPool = new BufferPool(processBuferPool, processBufferChunk,
				socketBufferLocalPercent / processorCount);
		businessExecutor = ExecutorUtil.create("BusinessExecutor",
				threadPoolSize);
		timerExecutor = ExecutorUtil.create("Timer", system.getTimerExecutor());
		listeningExecutorService = MoreExecutors.listeningDecorator(businessExecutor);

		for (int i = 0; i < processors.length; i++) {
			processors[i] = new NIOProcessor("Processor" + i, bufferPool,
					businessExecutor);
		}

		if (aio) {
			LOGGER.info("using aio network handler ");
			asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
			// startup connector
			connector = new AIOConnector();
			for (int i = 0; i < processors.length; i++) {
				asyncChannelGroups[i] = AsynchronousChannelGroup
						.withFixedThreadPool(processorCount,
								new ThreadFactory() {
									private int inx = 1;

									@Override
									public Thread newThread(Runnable r) {
										Thread th = new Thread(r);
										th.setName(BufferPool.LOCAL_BUF_THREAD_PREX
												+ "AIO" + (inx++));
										LOGGER.info("created new AIO thread "
												+ th.getName());
										return th;
									}
								});

			}
			manager = new AIOAcceptor(NAME + "Manager", system.getBindIp(),
					system.getManagerPort(), mf, this.asyncChannelGroups[0]);

			// startup server

			server = new AIOAcceptor(NAME + "Server", system.getBindIp(),
					system.getServerPort(), sf, this.asyncChannelGroups[0]);

		} else {
			LOGGER.info("using nio network handler ");
			NIOReactorPool reactorPool = new NIOReactorPool(
					BufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR",
					processors.length);
			connector = new NIOConnector(BufferPool.LOCAL_BUF_THREAD_PREX
					+ "NIOConnector", reactorPool);
			((NIOConnector) connector).start();

			manager = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME
					+ "Manager", system.getBindIp(), system.getManagerPort(),
					mf, reactorPool);

			server = new NIOAcceptor(BufferPool.LOCAL_BUF_THREAD_PREX + NAME
					+ "Server", system.getBindIp(), system.getServerPort(), sf,
					reactorPool);
		}
		// manager start
		manager.start();
		LOGGER.info(manager.getName() + " is started and listening on "
				+ manager.getPort());
		server.start();
		// server started
		LOGGER.info(server.getName() + " is started and listening on "
				+ server.getPort());
		LOGGER.info("===============================================");
		// init datahost
		Map<String, PhysicalDBPool> dataHosts = config.getDataHosts();
		LOGGER.info("Initialize dataHost ...");
		for (PhysicalDBPool node : dataHosts.values()) {
			String index = dnIndexProperties.getProperty(node.getHostName(),
					"0");
			if (!"0".equals(index)) {
				LOGGER.info("init datahost: " + node.getHostName()
						+ "  to use datasource index:" + index);
			}
			node.init(Integer.valueOf(index));
			node.startHeartbeat();
		}
		long dataNodeIldeCheckPeriod = system.getDataNodeIdleCheckPeriod();
		timer.schedule(updateTime(), 0L, TIME_UPDATE_PERIOD);
		timer.schedule(processorCheck(), 0L, system.getProcessorCheckPeriod());
		timer.schedule(dataNodeConHeartBeatCheck(dataNodeIldeCheckPeriod), 0L,
				dataNodeIldeCheckPeriod);
		timer.schedule(dataNodeHeartbeat(), 0L,
				system.getDataNodeHeartbeatPeriod());
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
			};
		};
	}

	private Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
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

	/**
	 * save cur datanode index to properties file
	 * 
	 * @param dataNode
	 * @param curIndex
	 */
	public synchronized void saveDataHostIndex(String dataHost, int curIndex) {

		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		FileOutputStream fileOut = null;
		try {
			String oldIndex = dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return;
			}
			dnIndexProperties.setProperty(dataHost, newIndex);
			LOGGER.info("save DataHost index  " + dataHost + " cur index "
					+ curIndex);

			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}

			fileOut = new FileOutputStream(file);
			dnIndexProperties.store(fileOut, "update");
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
							for (NIOProcessor p : processors) {
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

	public boolean isAIO() {
		return aio;
	}

	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}
}