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
package io.mycat.manager.handler;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.model.SystemConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.CircularArrayList;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ShowServerLog {
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final String DEFAULT_LOGFILE = "mycat.log";
    private static final Logger                LOGGER          = LoggerFactory
                                                                   .getLogger(ShowServerLog.class);
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("LOG", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	private static File getLogFile(String logFile) {

		String daasHome = SystemConfig.getHomePath();
		File file = new File(daasHome, "logs" + File.separator + logFile);
		return file;
	}

	public static void handle(String stmt,ManagerConnection c) {

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
		PackageBufINf bufInf = null;
		// show log key=warn limit=0,30
		Map<String, String> condPairMap = getCondPair(stmt);
		if (condPairMap.isEmpty()) {
			bufInf = showLogSum(c, buffer, packetId);
		} else {
			String logFile = condPairMap.get("file");
			if (logFile == null) {
				logFile = DEFAULT_LOGFILE;
			}
			String limitStr = condPairMap.get("limit");
			limitStr = (limitStr != null) ? limitStr : "0," + 100000;
			String[] limtArry = limitStr.split("\\s|,");
			int start = Integer.parseInt(limtArry[0]);
			int page = Integer.parseInt(limtArry[1]);
			int end = Integer.valueOf(start + page);
			String key = condPairMap.get("key");
			String regex = condPairMap.get("regex");
			bufInf = showLogRange(c, buffer, packetId, key, regex, start, end,
					logFile);

		}

		packetId = bufInf.packetId;
		buffer = bufInf.buffer;

		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// write buffer
		c.write(buffer);
	}

	public static PackageBufINf showLogRange(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String key, String regex,
			int start, int end, String logFile) {
		PackageBufINf bufINf = new PackageBufINf();
		Pattern pattern = null;
		if (regex != null) {
			pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		}
		if (key != null) {
			key = key.toLowerCase();
		}
		File file = getLogFile(logFile);
		BufferedReader br = null;
		int curLine = 0;
		try {
			br = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = br.readLine()) != null) {
				curLine++;
				if (curLine >= start && curLine <= end
						&& (
						(pattern != null && pattern.matcher(line).find())
								|| (pattern == null && key == null)
								|| (key != null && line.toLowerCase().contains(key))
						)) {
						RowDataPacket row = new RowDataPacket(FIELD_COUNT);
						row.add(StringUtil.encode(curLine + "->" + line,
								c.getCharset()));
						row.packetId = ++packetId;
						buffer = row.write(buffer, c,true);
				}
			}
			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;

		} catch (Exception e) {
            LOGGER.error("showLogRangeError", e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
		            LOGGER.error("showLogRangeError", e);
				}
			}

		}
		bufINf.packetId = packetId;
		return bufINf;
	}

	private static PackageBufINf showLogSum(ManagerConnection c,
			ByteBuffer buffer, byte packetId) {
		PackageBufINf bufINf = new PackageBufINf();
		File[] logFiles = new File(SystemConfig.getHomePath(), "logs")
				.listFiles();
		String fileNames = "";
		for (File f : logFiles) {
			if (f.isFile()) {
				fileNames += "  " + f.getName();
			}
		}

		File file = getLogFile(DEFAULT_LOGFILE);
		BufferedReader br = null;
		int totalLines = 0;
		CircularArrayList<String> queue = new CircularArrayList<String>(50);
		try {
			br = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = br.readLine()) != null) {
				totalLines++;
				if (queue.size() == queue.capacity()) {
					queue.remove(0);
				}
				queue.add(line);

			}

			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode("files in log dir:" + totalLines
					+ fileNames, c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode("Total lines " + totalLines + " ,tail "
					+ queue.size() + " line is following:", c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			int size = queue.size() - 1;
			for (int i = size; i >= 0; i--) {
				String data = queue.get(i);
				row = new RowDataPacket(FIELD_COUNT);
				row.add(StringUtil.encode(data, c.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, c,true);
			}
			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;

		} catch (Exception e) {
            LOGGER.error("showLogSumError", e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
		            LOGGER.error("showLogSumError", e);
				}
			}

		}
		bufINf.packetId = packetId;
		return bufINf;
	}

	public static Map<String, String> getCondPair(String sql) {
		HashMap<String, String> map = new HashMap<String, String>();
		Pattern p = Pattern.compile("(\\S+\\s*=\\s*\\S+)");
		Matcher m = p.matcher(sql);
		while (m.find()) {
			String item = m.group();
			Pattern p2 = Pattern.compile("(\\S+)\\s*=\\s*(\\S+)");
			Matcher m2 = p2.matcher(item);
			if (m2.find()) {
				map.put(m2.group(1), m2.group(2));
			}
		}
		return map;
	}

	public static void main(String[] args) {
		String sql = "show log limit =1,2 key=warn file= \"2\"  ";
		Map<String, String> condPairMap = getCondPair(sql);
		for (Map.Entry<String, String> entry : condPairMap.entrySet()) {
			System.out.println("key:" + entry.getKey() + ",value:"
					+ entry.getValue());

		}
		String limt = "1,2";
		System.out.println(Arrays.toString(limt.split("\\s|,")));

	}
}

class PackageBufINf {
	public byte packetId;
	public ByteBuffer buffer;
}