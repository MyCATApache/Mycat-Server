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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.util.ConfigUtil;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.StringUtil;

/**
 * Mycat conf file related Handler
 * 
 * @author wuzh
 */
public final class ConfFileHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ConfFileHandler.class);
	private static final int FIELD_COUNT = 1;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	private static final String UPLOAD_CMD = "FILE @@UPLOAD";
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = 1;

		fields[i] = PacketUtil.getField("DATA", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void handle( String stmt,ManagerConnection c) {
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
		String theStmt = stmt.toUpperCase().trim();
		PackageBufINf bufInf = null;
		if (theStmt.equals("FILE @@LIST")) {
			bufInf = listConfigFiles(c, buffer, packetId);
		} else if (theStmt.startsWith("FILE @@SHOW")) {
			int index = stmt.lastIndexOf(' ');
			String fileName = stmt.substring(index + 1);
			bufInf = showConfigFile(c, buffer, packetId, fileName);
		} else if (theStmt.startsWith(UPLOAD_CMD)) {
			int index = stmt.indexOf(' ', UPLOAD_CMD.length());
			int index2 = stmt.indexOf(' ', index + 1);
			if (index <= 0 || index2 <= 0 || index + 1 > stmt.length()
					|| index2 + 1 > stmt.length()) {
				bufInf = showInfo(c, buffer, packetId, "Invald param ,usage  ");
			}
			String fileName = stmt.substring(index + 1, index2);
			String content = stmt.substring(index2 + 1).trim();
			bufInf = upLoadConfigFile(c, buffer, packetId, fileName, content);
		} else {

			bufInf = showInfo(c, buffer, packetId, "Invald command ");
		}

		packetId = bufInf.packetId;
		buffer = bufInf.buffer;

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// write buffer
		c.write(buffer);
	}

	private static void checkXMLFile(String xmlFileName, byte[] data)
			throws ParserConfigurationException, SAXException, IOException {
		InputStream dtdStream = new ByteArrayInputStream(new byte[0]);
		File confDir = new File(SystemConfig.getHomePath(), "conf");
		if (xmlFileName.equals("schema.xml")) {
			dtdStream = MycatServer.class.getResourceAsStream("/schema.dtd");
			if (dtdStream == null) {
				dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
						confDir, "schema.dtd")));
			}

		} else if (xmlFileName.equals("server.xml")) {
			dtdStream = MycatServer.class.getResourceAsStream("/server.dtd");
			if (dtdStream == null) {
				dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
						confDir, "server.dtd")));
			}
		} else if (xmlFileName.equals("rule.xml")) {
			dtdStream = MycatServer.class.getResourceAsStream("/rule.dtd");
			if (dtdStream == null) {
				dtdStream = new ByteArrayInputStream(readFileByBytes(new File(
						confDir, "rule.dtd")));
			}
		}
		ConfigUtil.getDocument(dtdStream, new ByteArrayInputStream(data));
	}

	/**
	 * 以字节为单位读取文件，常用于读二进制文件，如图片、声音、影像等文件。
	 */
	private static byte[] readFileByBytes(File fileName) {
		InputStream in = null;
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		try { // 一次读多个字节
			byte[] tempbytes = new byte[100];
			int byteread = 0;
			in = new FileInputStream(fileName);
			// 读入多个字节到字节数组中，byteread为一次读入的字节数
			while ((byteread = in.read(tempbytes)) != -1) {
				outStream.write(tempbytes, 0, byteread);
			}
		} catch (Exception e1) {
		    LOGGER.error("readFileByBytesError",e1);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e1) {
				    LOGGER.error("readFileByBytesError",e1);
				}
			}
		}
		return outStream.toByteArray();
	}

	private static PackageBufINf upLoadConfigFile(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String fileName, String content) {
		LOGGER.info("Upload Daas Config file " + fileName + " ,content:"
				+ content);
		String tempFileName = System.currentTimeMillis() + "_" + fileName;
		File tempFile = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + tempFileName);
		BufferedOutputStream buff = null;
		boolean suc = false;
		try {
			byte[] fileData = content.getBytes("UTF-8");
			if (fileName.endsWith(".xml")) {
				checkXMLFile(fileName, fileData);
			}
			buff = new BufferedOutputStream(new FileOutputStream(tempFile));
			buff.write(fileData);
			buff.flush();

		} catch (Exception e) {
			LOGGER.warn("write file err " + e);
			return showInfo(c, buffer, packetId, "write file err " + e);

		} finally {
			if (buff != null) {
				try {
					buff.close();
					suc = true;
				} catch (IOException e) {
					LOGGER.warn("save config file err " + e);
				}
			}
		}
		if (suc) {
			// if succcess
			File oldFile = new File(SystemConfig.getHomePath(), "conf"
					+ File.separator + fileName);
			if (oldFile.exists()) {
				File backUP = new File(SystemConfig.getHomePath(), "conf"
						+ File.separator + fileName + "_"
						+ System.currentTimeMillis() + "_auto");
				if (!oldFile.renameTo(backUP)) {
					String msg = "rename old file failed";
					LOGGER.warn(msg + " for upload file "
							+ oldFile.getAbsolutePath());
					return showInfo(c, buffer, packetId, msg);
				}
			}
			File dest = new File(SystemConfig.getHomePath(), "conf"
					+ File.separator + fileName);
			if (!tempFile.renameTo(dest)) {
				String msg = "rename file failed";
				LOGGER.warn(msg + " for upload file "
						+ tempFile.getAbsolutePath());
				return showInfo(c, buffer, packetId, msg);
			}
			return showInfo(c, buffer, packetId, "SUCCESS SAVED FILE:"
					+ fileName);
		} else {
			return showInfo(c, buffer, packetId, "UPLOAD ERROR OCCURD:"
					+ fileName);
		}
	}

	private static PackageBufINf showInfo(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String string) {
		PackageBufINf bufINf = new PackageBufINf();
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(string, c.getCharset()));
		row.packetId = ++packetId;
		buffer = row.write(buffer, c,true);
		bufINf.packetId = packetId;
		bufINf.buffer = buffer;
		return bufINf;
	}

	private static PackageBufINf showConfigFile(ManagerConnection c,
			ByteBuffer buffer, byte packetId, String fileName) {
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + fileName);
		BufferedReader br = null;
		PackageBufINf bufINf = new PackageBufINf();
		try {
			br = new BufferedReader(new FileReader(file));
			String line = null;
			while ((line = br.readLine()) != null) {
				if (line.isEmpty()) {
					continue;
				}
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				row.add(StringUtil.encode(line, c.getCharset()));
				row.packetId = ++packetId;
				buffer = row.write(buffer, c,true);
			}
			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;

		} catch (Exception e) {
            LOGGER.error("showConfigFileError",e);
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
				    LOGGER.error("showConfigFileError",e);
				}
			}

		}
		bufINf.packetId = packetId;
		return bufINf;
	}

	private static PackageBufINf listConfigFiles(ManagerConnection c,
			ByteBuffer buffer, byte packetId) {
		PackageBufINf bufINf = new PackageBufINf();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		try {
			int i = 1;
			File[] file = new File(SystemConfig.getHomePath(), "conf")
					.listFiles();
			for (File f : file) {
				if (f.isFile()) {
					RowDataPacket row = new RowDataPacket(FIELD_COUNT);
					row.add(StringUtil.encode(
							(i++) + " : " + f.getName() + "  time:"
									+ df.format(new Date(f.lastModified())),
							c.getCharset()));
					row.packetId = ++packetId;
					buffer = row.write(buffer, c,true);
				}
			}

			bufINf.buffer = buffer;
			bufINf.packetId = packetId;
			return bufINf;

		} catch (Exception e) {
            LOGGER.error("listConfigFilesError",e);
			RowDataPacket row = new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode(e.toString(), c.getCharset()));
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
			bufINf.buffer = buffer;
		}
		bufINf.packetId = packetId;
		return bufINf;
	}

	public static void main(String[] args) {
		String stmt = "FILE @@UPLOAD test.xml 1234567890";
		int index = stmt.indexOf(' ', UPLOAD_CMD.length());
		int index2 = stmt.indexOf(' ', index + 1);
		if (index <= 0 || index2 <= 0 || index + 1 > stmt.length()
				|| index2 + 1 > stmt.length()) {
			System.out.println("valid ....");
		} else {
			String fileName = stmt.substring(index + 1, index2);
			String content = stmt.substring(index2 + 1).trim();
			System.out.println(fileName + " content:" + content);
		}
	}

}