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
package io.mycat.manager;

import io.mycat.MycatServer;
import io.mycat.config.MycatPrivileges;
import io.mycat.net.FrontendConnection;
import io.mycat.net.factory.FrontendConnectionFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;

/**
 * 前端管理器连接工厂 管理请求
 * @author mycat
 */
public class ManagerConnectionFactory extends FrontendConnectionFactory {

    /**
     * 获取前端连接
     * @param channel
     * @return
     * @throws IOException
     */
    @Override
    protected FrontendConnection getConnection(NetworkChannel channel) throws IOException {
        ManagerConnection c = new ManagerConnection(channel);
        // 设置连接参数
        MycatServer.getInstance().getConfig().setSocketParams(c, true);
        // 设置MyCat前端权限
        c.setPrivileges(MycatPrivileges.instance());
        c.setQueryHandler(new ManagerQueryHandler(c));
        return c;
    }

}