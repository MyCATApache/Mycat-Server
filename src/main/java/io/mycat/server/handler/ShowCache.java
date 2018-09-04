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
package io.mycat.server.handler;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.cache.CachePool;
import io.mycat.cache.CacheService;
import io.mycat.cache.CacheStatic;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;

public class ShowCache {

	private static final int FIELD_COUNT = 8;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("CACHE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("MAX", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("CUR", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("ACCESS", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("HIT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("PUT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("LAST_ACCESS", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("LAST_PUT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {

		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c,true);
		}

		// write eof
		buffer = eof.write(buffer, c,true);

		// write rows
		byte packetId = eof.packetId;
		CacheService cacheService = MycatServer.getInstance().getCacheService();
		for (Map.Entry<String, CachePool> entry : cacheService
				.getAllCachePools().entrySet()) {
			String cacheName=entry.getKey();
			CachePool cachePool = entry.getValue();
			if (cachePool instanceof LayerCachePool) {
				for (Map.Entry<String, CacheStatic> staticsEntry : ((LayerCachePool) cachePool)
						.getAllCacheStatic().entrySet()) {
					RowDataPacket row = getRow(cacheName+'.'+staticsEntry.getKey(),
							staticsEntry.getValue(), c.getCharset());
					row.packetId = ++packetId;
					buffer = row.write(buffer, c,true);
				}
			} else {
				RowDataPacket row = getRow(cacheName,
						cachePool.getCacheStatic(), c.getCharset());
				row.packetId = ++packetId;
				buffer = row.write(buffer, c,true);
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(String poolName,
			CacheStatic cacheStatic, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(poolName, charset));
		// max size
		row.add(LongUtil.toBytes(cacheStatic.getMaxSize()));
		row.add(LongUtil.toBytes(cacheStatic.getItemSize()));
		row.add(LongUtil.toBytes(cacheStatic.getAccessTimes()));
		row.add(LongUtil.toBytes(cacheStatic.getHitTimes()));
		row.add(LongUtil.toBytes(cacheStatic.getPutTimes()));
		row.add(LongUtil.toBytes(cacheStatic.getLastAccesTime()));
		row.add(LongUtil.toBytes(cacheStatic.getLastPutTime()));
		return row;
	}

}