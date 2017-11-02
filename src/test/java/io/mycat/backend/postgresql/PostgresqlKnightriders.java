package io.mycat.backend.postgresql;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.DataRow.DataColumn;
import io.mycat.backend.postgresql.packet.Parse;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket.DateType;
import io.mycat.backend.postgresql.packet.Query;
import io.mycat.backend.postgresql.packet.Terminate;
import io.mycat.backend.postgresql.utils.PIOUtils;
import io.mycat.backend.postgresql.utils.PacketUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/*************
 * 提交代码..
 * 
 * @author Coollf
 *
 */
public class PostgresqlKnightriders {

	private static Logger logger = LoggerFactory
			.getLogger(PostgresqlKnightriders.class);

	public static void main(String[] args) {

		List<String[]> paramList = new ArrayList<String[]>();
		String user = "postgres";
		String password = "coollf";
		String database = "mycat";
		String appName = "MyCat-Server";

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

				SocketChannel channel = SocketChannel
						.open(new InetSocketAddress("localhost", 5210));

				channel.configureBlocking(false);

				// 打开并注册选择器到信道
				Selector selector = Selector.open();
				channel.register(selector, SelectionKey.OP_READ
						| SelectionKey.OP_WRITE);

				// 启动读取线程
				new TCPClientReadThread(selector);

				// sendStartupPacket(channel, paramList.toArray(new
				// String[0][]));
				ByteBuffer in = ByteBuffer.allocate(10);
				channel.read(in);
				// System.out.println(in);

			} else {
				sendStartupPacket(socket, paramList.toArray(new String[0][]));
				PostgreSQLPacket packet = readParsePacket(socket).get(0);
				if (packet instanceof AuthenticationPacket) {
					AuthType aut = ((AuthenticationPacket) packet)
							.getAuthType();
					if (aut != AuthType.Ok) {
						PasswordMessage pak = new PasswordMessage(user,
								password, aut,
								((AuthenticationPacket) packet).getSalt());
						ByteBuffer buffer = ByteBuffer
								.allocate(pak.getLength() + 1);
						pak.write(buffer);
						socket.getOutputStream().write(buffer.array());
						List<PostgreSQLPacket> sqlPacket = readParsePacket(socket);
						System.out.println(JSON.toJSONString(sqlPacket));
						int pid = 0;
						int secretKey = 0;
						for (PostgreSQLPacket p : sqlPacket) {
							if (p instanceof BackendKeyData) {
								pid = ((BackendKeyData) p).getPid();
								secretKey = ((BackendKeyData) p).getSecretKey();
							}
						}

						Query query = new Query(
								"SELECT text_,timestamp_ from ump_types");
						// Query query = new Query("SELECT 1"+"\0");

						ByteBuffer oby = ByteBuffer
								.allocate(query.getLength() + 1);
						query.write(oby);

						socket.getOutputStream().write(oby.array());

						sqlPacket = readParsePacket(socket);
						for (PostgreSQLPacket p : sqlPacket) {
							if (p instanceof DataRow) {
								for (DataColumn c : ((DataRow) p).getColumns()) {
									System.out.println(new String(c.getData(),
											"utf-8"));
								}
							}
						}
						System.out.println(JSON.toJSONString(sqlPacket));
						query = new Query("");
						oby = ByteBuffer.allocate(query.getLength() + 1);
						query.write(oby);

						socket.getOutputStream().write(oby.array());

						sqlPacket = readParsePacket(socket);
						System.out.println(JSON.toJSONString(sqlPacket));

						// CancelRequest cancelRequest = new CancelRequest(pid,
						// secretKey);
						// oby = ByteBuffer.allocate(cancelRequest.getLength());
						// cancelRequest.write(oby);
						// socket.getOutputStream().write(oby.array());
						// List<PostgreSQLPacket> pgs = readParsePacket(socket);
						// System.out.println(JSON.toJSONString(pgs));

						// 解析sql
						String uuid = UUID.randomUUID().toString();

						String sql = "INSERT into ump_coupon(id_,name_,time) VALUES (4 , ? , now());";
						Parse parse = new Parse(null, sql,DateType.UNKNOWN);
						oby = ByteBuffer.allocate(parse.getPacketSize());

						parse.write(oby);
						socket.getOutputStream().write(oby.array());
						socket.getOutputStream().write(new byte[]{0});
						List<PostgreSQLPacket> tre = readParsePacket(socket);
						System.out.println(JSON.toJSONString(tre));

//						Terminate terminate = new Terminate();
//						oby = ByteBuffer.allocate(terminate.getLength() + 1);
//						terminate.write(oby);
//						socket.getOutputStream().write(oby.array());
						tre = readParsePacket(socket);
						System.out.println(tre);

					}
				}
			}

			System.in.read();
			System.in.read();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<PostgreSQLPacket> readParsePacket(Socket socket)
			throws IOException, IllegalAccessException {
		byte[] bytes = new byte[1024 * 10];
		int leg = socket.getInputStream().read(bytes, 0, bytes.length);

		int offset = 0;
		return PacketUtils.parsePacket(bytes, offset, leg);
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

	private static void sendStartupPacket(Socket socket, String[][] params)
			throws IOException {
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
			length += encodedParams[i * 2].length + 1
					+ encodedParams[i * 2 + 1].length + 1;
		}

		length += 1; // Terminating \0

		ByteBuffer buffer = ByteBuffer.allocate(length);

		/*
		 * Send the startup message.
		 */
		PIOUtils.SendInteger4(length, buffer);
		PIOUtils.SendInteger2(3, buffer); // protocol major
		PIOUtils.SendInteger2(0, buffer); // protocol minor
		for (byte[] encodedParam : encodedParams) {
			PIOUtils.Send(encodedParam, buffer);
			PIOUtils.SendChar(0, buffer);
		}
		sout.write(buffer.array());
	}

}

class TCPClientReadThread implements Runnable {

	private static Logger logger = LoggerFactory
			.getLogger(TCPClientReadThread.class);
	private Selector selector;

	
	private ByteBuffer bs ;
	
	public TCPClientReadThread(Selector selector) {
		this.selector = selector;

		new Thread(this).start();
	}

	public void run() {
		boolean a = false;
		try {
			while (selector.select() > 0) {
				System.out.println(".....");
				// 遍历每个有可用IO操作Channel对应的SelectionKey
				for (SelectionKey sk : selector.selectedKeys()) {

					if (sk.isWritable()) {
						SocketChannel sc = (SocketChannel) sk.channel();
						if (!a) {
							sendStartupPacket(sc);
							a = true;
						}
						if(this.bs!= null){
							sc.write(bs);
						}
						// 删除正在处理的SelectionKey
						// selector.selectedKeys().remove(sk);
						sk.interestOps(SelectionKey.OP_READ);
					}
					if (sk.isReadable()) {
						// 使用NIO读取Channel中的数据
						SocketChannel sc = (SocketChannel) sk.channel();
						ByteBuffer buffer = ByteBuffer.allocate(1024);
						sc.read(buffer);
						buffer.flip();

						byte[] array = buffer.array();
						List<PostgreSQLPacket> ls = PacketUtils.parsePacket(
								array, 0, buffer.limit());
						if (ls.size() > 0) {
							if (ls.get(0) instanceof AuthenticationPacket) {
								AuthenticationPacket aut = (AuthenticationPacket) ls
										.get(0);
								if (aut.getAuthType() != AuthType.Ok) {
									PasswordMessage pak = new PasswordMessage(
											"postgres", "coollf",
											aut.getAuthType(), aut.getSalt());
									ByteBuffer _buffer = ByteBuffer
											.allocate(pak.getLength() + 2);
									pak.write(_buffer);
									//_buffer.put((byte)0);
									_buffer.flip();
									this.bs = _buffer;
								//	sk.interestOps(SelectionKey.OP_READ);
								}else{
									logger.error("登陆成功啦啦啦....");
								}
							}
						}
						sk.interestOps(SelectionKey.OP_WRITE);

						// 控制台打印出来
						System.out.println("接收到来自服务器" + JSON.toJSONString(ls));

						// 为下一次读取作准备
					//	sk.interestOps(SelectionKey.OP_WRITE);
					}

				}
			}
			System.out.println("熄火了.....");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private static void sendStartupPacket(SocketChannel socketChannel)
			throws IOException {
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
			length += encodedParams[i * 2].length + 1
					+ encodedParams[i * 2 + 1].length + 1;
		}

		length += 1; // Terminating \0

		ByteBuffer buffer = ByteBuffer.allocate(length);

		/*
		 * Send the startup message.
		 */
		PIOUtils.SendInteger4(length, buffer);
		PIOUtils.SendInteger2(3, buffer); // protocol major
		PIOUtils.SendInteger2(0, buffer); // protocol minor
		for (byte[] encodedParam : encodedParams) {
			PIOUtils.Send(encodedParam, buffer);
			PIOUtils.SendChar(0, buffer);
		}
		PIOUtils.Send(new byte[] { 0 }, buffer);

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