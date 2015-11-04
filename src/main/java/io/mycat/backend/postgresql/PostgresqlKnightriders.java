package io.mycat.backend.postgresql;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Buffer;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.packet.PasswordMessage;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.utils.MD5Digest;
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
		String password  =  "coollf";
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
		
		try {
			Socket socket = new Socket("localhost", 5432);
			sendStartupPacket(socket, paramList.toArray(new String[0][]));
			PostgreSQLPacket packet = rec(socket);
			if(packet instanceof AuthenticationPacket){
			  AuthType aut = ((AuthenticationPacket) packet).getAuthType();
			  if(aut != AuthType.Ok){
				  PasswordMessage pak = new PasswordMessage(user,password,aut,((AuthenticationPacket) packet).getSalt());
				  ByteBuffer buffer = ByteBuffer.allocate(1024);
				  pak.write(buffer );
				  socket.getOutputStream().write(buffer.array());
				  
				  packet = rec(socket);
				  if(packet instanceof AuthenticationPacket && ((AuthenticationPacket) packet).getAuthType() == AuthType.Ok){
					  System.out.println("登入成功啦.....");
				  }
			  }
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	private static PostgreSQLPacket rec(Socket socket) throws IOException, IllegalAccessException {
		ByteBuffer buffer = ByteBuffer.allocateDirect(10);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		byte[] bytes = new byte[100];
		int count = 0;
		int leg = socket.getInputStream().read(bytes, 0, bytes.length);
		char MAKE = (char) bytes[0];
		switch (MAKE) {
		case 'R':
			return AuthenticationPacket.parse(bytes);
		}

		return null;
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
