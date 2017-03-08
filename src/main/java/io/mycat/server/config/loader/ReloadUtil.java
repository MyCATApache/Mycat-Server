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
package io.mycat.server.config.loader;


import io.mycat.MycatServer;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.cluster.MycatClusterConfig;
import io.mycat.server.config.node.CharsetConfig;
import io.mycat.server.config.node.HostIndexConfig;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.config.node.QuarantineConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.SequenceConfig;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.config.node.UserConfig;
import io.mycat.server.response.ReloadCallBack;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author mycat
 */
public final class ReloadUtil {
	public static void execute(MySQLFrontConnection c, final boolean loadAll) {
		final ReentrantLock lock = MycatServer.getInstance().getConfig().getLock();
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
		reload();

		//加载数据源
		boolean reloadStatus = true;
		MycatConfig conf = MycatServer.getInstance().getConfig();
		reloadStatus = conf.reloadDatasource();
		if(!reloadStatus){
			return false;
		}

		return true;
	}

    private static boolean reload() {
        // 载入新的配置
        ConfigInitializer loader = new ConfigInitializer(false);
        Map<String, UserConfig> users = loader.getUsers();
        Map<String, SchemaConfig> schemas = loader.getSchemas();
        Map<String, PhysicalDBNode> dataNodes = loader.getDataNodes();
        Map<String, PhysicalDBPool> dataHosts = loader.getDataHosts();
        MycatClusterConfig cluster = loader.getCluster();
        QuarantineConfig quarantine = loader.getQuarantine();
        CharsetConfig charsetConfig = loader.getCharsetConfig();
        SequenceConfig sequenceConfig = loader.getSequenceConfig();
        HostIndexConfig hostIndexConfig = loader.getHostIndexs();

        // 应用新配置
        MycatServer instance = MycatServer.getInstance();
        MycatConfig conf = instance.getConfig();
        conf.reloadCharsetConfigs();

        // 应用重载
        conf.reload(users, schemas, dataNodes, dataHosts, cluster, quarantine,
        		charsetConfig,sequenceConfig,hostIndexConfig,false);

        //清理缓存
        instance.getCacheService().clearCache();
        return true;
    }

    public static boolean rollback() {
		MycatConfig conf = MycatServer.getInstance().getConfig();
		Map<String, UserConfig> users = conf.getBackupUsers();
		Map<String, SchemaConfig> schemas = conf.getBackupSchemas();
		Map<String, PhysicalDBNode> dataNodes = conf.getBackupDataNodes();
		Map<String, PhysicalDBPool> dataHosts = conf.getBackupDataHosts();
		MycatClusterConfig cluster = conf.getBackupCluster();
		QuarantineConfig quarantine = conf.getBackupQuarantine();
		CharsetConfig charsetConfig = conf.getBackupCharsetConfig();
        SequenceConfig sequenceConfig = conf.getBackupSequenceConfig();
        HostIndexConfig hostIndexConfig = conf.getBackupHostIndexs();

		// 检查可回滚状态
		if (!conf.canRollback()) {
			return false;
		}
		// 应用回滚
		conf.rollback(users, schemas, dataNodes, dataHosts, cluster, quarantine,charsetConfig,sequenceConfig,hostIndexConfig);

		conf.rebackDatasource();

		//清理缓存
		MycatServer.getInstance().getCacheService().clearCache();
		return true;
	}


}