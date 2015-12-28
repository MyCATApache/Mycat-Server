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
package org.opencloudb.response;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.log4j.Logger;
import org.opencloudb.ConfigInitializer;
import org.opencloudb.MycatCluster;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.backend.PhysicalDBPool;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.DnPropertyUtil;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;

/**
 * @author mycat
 */
public final class ReloadConfig {
	private static final Logger LOGGER = Logger.getLogger(ReloadConfig.class);

	public static void execute(ManagerConnection c, final boolean loadAll) {
		final ReentrantLock lock = MycatServer.getInstance().getConfig()
				.getLock();
		lock.lock();
		try {
			ListenableFuture<Boolean> listenableFuture = MycatServer.getInstance().getListeningExecutorService().submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call() throws Exception
                {

                    return loadAll?reload_all():reload();
                }
            });
			Futures.addCallback(listenableFuture, new ReloadCallBack(c),  MycatServer.getInstance().getListeningExecutorService());
		} finally {
			lock.unlock();
		}
	}

	private static boolean reload_all() {
		// 载入新的配置
		ConfigInitializer loader = new ConfigInitializer(true);
		Map<String, UserConfig> users = loader.getUsers();
		Map<String, SchemaConfig> schemas = loader.getSchemas();
		Map<String, PhysicalDBNode> dataNodes = loader.getDataNodes();
		Map<String, PhysicalDBPool> dataHosts = loader.getDataHosts();
		MycatCluster cluster = loader.getCluster();
		QuarantineConfig quarantine = loader.getQuarantine();

		// 应用新配置
		MycatConfig conf = MycatServer.getInstance().getConfig();
		conf.setDataNodes(dataNodes);
		
		Map<String, PhysicalDBPool> cNodes = conf.getDataHosts();
		boolean reloadStatus = true;
		for (PhysicalDBPool dn : dataHosts.values()) {
			dn.setSchemas(MycatServer.getInstance().getConfig().getDataNodeSchemasOfDataHost(dn.getHostName()));
			// init datahost
			String index = DnPropertyUtil.loadDnIndexProps().getProperty(dn.getHostName(),
					"0");
			if (!"0".equals(index)) {
				LOGGER.info("init datahost: " + dn.getHostName()
						+ "  to use datasource index:" + index);
			}
			dn.init(Integer.valueOf(index));

			//dn.init(0);
			if (!dn.isInitSuccess()) {
				reloadStatus = false;
				break;
			}
		}
		// 如果重载不成功，则清理已初始化的资源。
		if (!reloadStatus) {
			LOGGER.warn("reload failed ,clear previously created datasources ");
			for (PhysicalDBPool dn : dataHosts.values()) {
				dn.clearDataSources("reload config");
				dn.stopHeartbeat();
			}
			return false;
		}

		// 应用重载
		conf.reload(users, schemas, dataNodes, dataHosts, cluster, quarantine,true);

		// 处理旧的资源
		for (PhysicalDBPool dn : cNodes.values()) {
			dn.clearDataSources("reload config clear old datasources");
			dn.stopHeartbeat();
		}

		//清理缓存
		 MycatServer.getInstance().getCacheService().clearCache();
		return true;
	}

    private static boolean reload() {
        // 载入新的配置
        ConfigInitializer loader = new ConfigInitializer(false);
        Map<String, UserConfig> users = loader.getUsers();
        Map<String, SchemaConfig> schemas = loader.getSchemas();
        Map<String, PhysicalDBNode> dataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> dataHosts = loader.getDataHosts();
        MycatCluster cluster = loader.getCluster();
        QuarantineConfig quarantine = loader.getQuarantine();

        // 应用新配置
        MycatServer instance = MycatServer.getInstance();
        MycatConfig conf = instance.getConfig();

        // 应用重载
        conf.reload(users, schemas, dataNodes, dataHosts, cluster, quarantine,false);


        //清理缓存
        instance.getCacheService().clearCache();
        return true;
    }
	/**
	 * 异步执行回调类，用于回写数据给用户等。
	 */
	private static class ReloadCallBack implements FutureCallback<Boolean> {

		private ManagerConnection mc;

		private ReloadCallBack(ManagerConnection c) {
			this.mc = c;
		}

		@Override
		public void onSuccess(Boolean result) {
			if (result) {
				LOGGER.warn("send ok package to client " + String.valueOf(mc));
				OkPacket ok = new OkPacket();
				ok.packetId = 1;
				ok.affectedRows = 1;
				ok.serverStatus = 2;
				ok.message = "Reload config success".getBytes();
				ok.write(mc);
			} else {
				mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
			}
		}

		@Override
		public void onFailure(Throwable t) {
			mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
		}
	}
}