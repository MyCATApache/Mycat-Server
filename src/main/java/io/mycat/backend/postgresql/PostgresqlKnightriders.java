package io.mycat.backend.postgresql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.ErrorResponse;
import io.mycat.backend.postgresql.packet.NoticeResponse;
import io.mycat.backend.postgresql.packet.ParameterStatus;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.Query;
import io.mycat.backend.postgresql.packet.ReadyForQuery;
import io.mycat.backend.postgresql.utils.PostgreSQLIOUtils;

/*************
 * 提交代码..
 * 
 * @author Coollf
 *
 */
public class PostgresqlKnightriders {

	private static Logger logger = LoggerFactory.getLogger(PostgresqlKnightriders.class);

	public static void main(String[] args) {

		List<String[]> paramList = new ArrayList<String[]>();
		String user = "postgres";
		String password = "coollf";
		String database = "Coollf";
		String appName = "MyCat-Server";
		String assumeMinServerVersion = "9.0.0";

		paramList.add(new String[] { "user", user });
		paramList.add(new String[] { "database", database });
		paramList.add(new String[] { "client_encoding", "UTF8" });
		paramList.add(new String[] { "DateStyle", "ISO" });
		paramList.add(new String[] { "TimeZone", createPostgresTimeZone() });
		paramList.add(new String[] { "extra_float_digits", "3" });
		paramList.add(new String[] { "application_name", appName });

		boolean nio = false;

		try {
			Socket socket = new Socket("localhost", 5432);
			if (nio) {

				SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 5432));

				channel.configureBlocking(false);

				// 打开并注册选择器到信道
				Selector selector = Selector.open();
				channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

				// 启动读取线程
				new TCPClientReadThread(selector);

				// sendStartupPacket(channel, paramList.toArray(new
				// String[0][]));
				ByteBuffer in = ByteBuffer.allocate(10);
				channel.read(in);
				System.out.println(in);

			} else {
				sendStartupPacket(socket, paramList.toArray(new String[0][]));
				PostgreSQLPacket packet = rec(socket).get(0);
				if (packet instanceof AuthenticationPacket) {
					AuthType aut = ((AuthenticationPacket) packet).getAuthType();
					if (aut != AuthType.Ok) {
						PasswordMessage pak = new PasswordMessage(user, password, aut,
								((AuthenticationPacket) packet).getSalt());
						ByteBuffer buffer = ByteBuffer.allocate(pak.getLength() + 1);
						pak.write(buffer);
						socket.getOutputStream().write(buffer.array());
						List<PostgreSQLPacket> sqlPacket = rec(socket);
						System.out.println(JSON.toJSONString(sqlPacket));
						
						Query query = new Query("SELECT * from  ump_coupon"+"\0");
						
						ByteBuffer oby = ByteBuffer.allocate(query.getLength() + 1);
						query.write(oby);
						
						socket.getOutputStream().write(oby.array());
						
						sqlPacket = rec(socket);
						System.out.println(JSON.toJSONString(sqlPacket));
						
					}
				}
			}

			System.in.read();
			System.in.read();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<PostgreSQLPacket> rec(Socket socket) throws IOException, IllegalAccessException {
		byte[] bytes = new byte[1024];
		int leg = socket.getInputStream().read(bytes, 0, bytes.length);
		List<PostgreSQLPacket> pgs = new ArrayList<>();
		int offset = 0;
		while (offset < leg) {
			char MAKE = (char) bytes[offset];
			PostgreSQLPacket pg = null;
			switch (MAKE) {
			case 'R':
				pg = AuthenticationPacket.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			case 'E':
				pg = ErrorResponse.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			case 'K':
				pg = BackendKeyData.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			case 'S':
				pg = ParameterStatus.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			case 'Z':
				pg = ReadyForQuery.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			case 'N':
				pg = NoticeResponse.parse(ByteBuffer.wrap(bytes, offset, leg-offset), offset);
				break;
			}
			if (pg != null) {
				offset = offset + pg.getLength() + 1;
				pgs.add(pg);
			}
		}

		return pgs;
	}

	/**
	 * Convert Java time zone to postgres time zone. All others stay the same
	 * except that GMT+nn changes to GMT-nn and vise versa.
	 * 
	 * @return The current JVM time zone in postgresql format.
	 */
	private static String createPostgresTimeZone() {
		String tz = TimeZone.getDefault().getID();
		if (tz.length() <= 3 || !tz.startsWith("GMT")) {
			return tz;
		}
		char sign = tz.charAt(3);
		String start;
		if (sign == '+') {
			start = "GMT-";
		} else if (sign == '-') {
			start = "GMT+";
		} else {
			// unknown type
			return tz;
		}

		return start + tz.substring(4);
	}

	private static void sendStartupPacket(Socket socket, String[][] params) throws IOException {
		OutputStream sout = socket.getOutputStream();
		if (logger.isDebugEnabled()) {
			StringBuilder details = new StringBuilder();
			for (int i = 0; i < params.length; ++i) {
				if (i != 0)
					details.append(", ");
				details.append(params[i][0]);
				details.append("=");
				details.append(params[i][1]);
			}
			logger.debug(" FE=> StartupPacket(" + details + ")");
		}

		/*
		 * Precalculate message length and encode params.
		 */
		int length = 4 + 4;
		byte[][] encodedParams = new byte[params.length * 2][];
		for (int i = 0; i < params.length; ++i) {
			encodedParams[i * 2] = params[i][0].getBytes("UTF-8");
			encodedParams[i * 2 + 1] = params[i][1].getBytes("UTF-8");
			length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
		}

		length += 1; // Terminating \0

		ByteBuffer buffer = ByteBuffer.allocate(length);

		/*
		 * Send the startup message.
		 */
		PostgreSQLIOUtils.SendInteger4(length, buffer);
		PostgreSQLIOUtils.SendInteger2(3, buffer); // protocol major
		PostgreSQLIOUtils.SendInteger2(0, buffer); // protocol minor
		for (byte[] encodedParam : encodedParams) {
			PostgreSQLIOUtils.Send(encodedParam, buffer);
			PostgreSQLIOUtils.SendChar(0, buffer);
		}
		sout.write(buffer.array());
	}

}

class TCPClientReadThread implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(TCPClientReadThread.class);
	private Selector selector;

	public TCPClientReadThread(Selector selector) {
		this.selector = selector;

		new Thread(this).start();
	}

	public void run() {
		boolean a = false;
		try {
			while (selector.select() > 0) {
				// 遍历每个有可用IO操作Channel对应的SelectionKey
				for (SelectionKey sk : selector.selectedKeys()) {

					if (sk.isWritable()) {
						SocketChannel sc = (SocketChannel) sk.channel();
						if (!a) {
							sendStartupPacket(sc);
							a = true;
						}
					} else if (sk.isReadable()) {
						// 使用NIO读取Channel中的数据
						SocketChannel sc = (SocketChannel) sk.channel();
						ByteBuffer buffer = ByteBuffer.allocate(1024);
						sc.read(buffer);
						buffer.flip();

						// 将字节转化为为UTF-16的字符串
						String receivedString = Charset.forName("UTF-18").newDecoder().decode(buffer).toString();

						// 控制台打印出来
						System.out.println("接收到来自服务器" + sc.socket().getRemoteSocketAddress() + "的信息:" + receivedString);

						// 为下一次读取作准备
						sk.interestOps(SelectionKey.OP_READ);
					}

					// 删除正在处理的SelectionKey
					selector.selectedKeys().remove(sk);
				}
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private static void sendStartupPacket(SocketChannel socketChannel) throws IOException {
		List<String[]> paramList = new ArrayList<String[]>();
		String user = "postgres";
		String password = "coollf";
		String database = "odoo";
		String appName = "MyCat-Server";
		String assumeMinServerVersion = "9.0.0";

		paramList.add(new String[] { "user", user });
		paramList.add(new String[] { "database", database });
		paramList.add(new String[] { "client_encoding", "UTF8" });
		paramList.add(new String[] { "DateStyle", "ISO" });
		paramList.add(new String[] { "TimeZone", createPostgresTimeZone() });
		paramList.add(new String[] { "extra_float_digits", "3" });
		paramList.add(new String[] { "application_name", appName });

		String[][] params = paramList.toArray(new String[0][]);

		if (logger.isDebugEnabled()) {
			StringBuilder details = new StringBuilder();
			for (int i = 0; i < params.length; ++i) {
				if (i != 0)
					details.append(", ");
				details.append(params[i][0]);
				details.append("=");
				details.append(params[i][1]);
			}
			logger.debug(" FE=> StartupPacket(" + details + ")");
		}

		/*
		 * Precalculate message length and encode params.
		 */
		int length = 4 + 4;
		byte[][] encodedParams = new byte[params.length * 2][];
		for (int i = 0; i < params.length; ++i) {
			encodedParams[i * 2] = params[i][0].getBytes("UTF-8");
			encodedParams[i * 2 + 1] = params[i][1].getBytes("UTF-8");
			length += encodedParams[i * 2].length + 1 + encodedParams[i * 2 + 1].length + 1;
		}

		length += 1; // Terminating \0

		ByteBuffer buffer = ByteBuffer.allocate(length);

		/*
		 * Send the startup message.
		 */
		PostgreSQLIOUtils.SendInteger4(length, buffer);
		PostgreSQLIOUtils.SendInteger2(3, buffer); // protocol major
		PostgreSQLIOUtils.SendInteger2(0, buffer); // protocol minor
		for (byte[] encodedParam : encodedParams) {
			PostgreSQLIOUtils.Send(encodedParam, buffer);
			PostgreSQLIOUtils.SendChar(0, buffer);
		}
		buffer.flip();
		socketChannel.write(buffer);

	}

	/**
	 * Convert Java time zone to postgres time zone. All others stay the same
	 * except that GMT+nn changes to GMT-nn and vise versa.
	 * 
	 * @return The current JVM time zone in postgresql format.
	 */
	private static String createPostgresTimeZone() {
		String tz = TimeZone.getDefault().getID();
		if (tz.length() <= 3 || !tz.startsWith("GMT")) {
			return tz;
		}
		char sign = tz.charAt(3);
		String start;
		if (sign == '+') {
			start = "GMT-";
		} else if (sign == '-') {
			start = "GMT+";
		} else {
			// unknown type
			return tz;
		}

		return start + tz.substring(4);
	}
}