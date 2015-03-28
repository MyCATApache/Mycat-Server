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
package org.opencloudb.server.handler;

import com.google.common.io.Files;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.opencloudb.net.handler.LoadDataInfileHandler;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.opencloudb.server.ServerConnection;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author mycat
 */
public final class ServerLoadDataInfileHandler implements LoadDataInfileHandler
{
    private ServerConnection serverConnection;
    private String sql;
    private String fileName;
    private long affectedRows;

    public ServerLoadDataInfileHandler(ServerConnection serverConnection)
    {
        this.serverConnection = serverConnection;

    }

    @Override
    public void start(String sql)
    {
        this.sql = sql;
        ByteBuffer buffer = serverConnection.allocate();
        RequestFilePacket filePacket = new RequestFilePacket();
        filePacket.fileName = "sql.csv".getBytes();
        filePacket.packetId=1;
        filePacket.write(buffer, serverConnection, true);

    }

    @Override
    public void execute(byte[] data)
    {

            try
            {
                BinaryPacket packet=new BinaryPacket();
                ByteInputStream inputStream=new ByteInputStream(data,data.length);
                packet.read(inputStream);
                Files.write(packet.data, new File("d:\\88\\mycat.txt"));

            } catch (IOException e)
            {
              throw new RuntimeException(e);
            }

    }

    @Override
    public void end()
    {
        affectedRows=10;
        //load in data空包 结束
        OkPacket ok = new OkPacket();
        ok.packetId = 4;
        ok.affectedRows = affectedRows;

       ok.serverStatus = serverConnection.isAutocommit() ? 0x0002: 0x0001;
        ok.message=new byte[]{0};
        ok.write(serverConnection);
        sql = null;
    }
}