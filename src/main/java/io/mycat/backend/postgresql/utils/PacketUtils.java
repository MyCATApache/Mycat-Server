package io.mycat.backend.postgresql.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import io.mycat.backend.postgresql.packet.AuthenticationPacket;
import io.mycat.backend.postgresql.packet.BackendKeyData;
import io.mycat.backend.postgresql.packet.CommandComplete;
import io.mycat.backend.postgresql.packet.CopyInResponse;
import io.mycat.backend.postgresql.packet.CopyOutResponse;
import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.EmptyQueryResponse;
import io.mycat.backend.postgresql.packet.ErrorResponse;
import io.mycat.backend.postgresql.packet.NoticeResponse;
import io.mycat.backend.postgresql.packet.ParameterStatus;
import io.mycat.backend.postgresql.packet.ParseComplete;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket;
import io.mycat.backend.postgresql.packet.ReadyForQuery;
import io.mycat.backend.postgresql.packet.RowDescription;

public class PacketUtils {

	public static List<PostgreSQLPacket> parsePacket(ByteBuffer buffer,int offset,int readLength) throws  IOException{
		final ByteBuffer bytes = buffer;
		List<PostgreSQLPacket> pgs = new ArrayList<>();
		while(offset < readLength){
			char MAKE = (char)bytes.get(offset);
			PostgreSQLPacket pg = null;
			switch (MAKE) {
			case 'R':
				pg = AuthenticationPacket.parse(bytes, offset);
				break;
			case 'E':
				pg = ErrorResponse.parse(bytes, offset);
				break;
			case 'K':
				pg = BackendKeyData.parse(bytes, offset);
				break;
			case 'S':
				pg = ParameterStatus.parse(bytes, offset);
				break;
			case 'Z':
				pg = ReadyForQuery.parse(bytes, offset);
				break;
			case 'N':
				pg = NoticeResponse.parse(bytes, offset);
				break;
			case 'C':
				pg = CommandComplete.parse(bytes, offset);
				break;
			case 'T':
				pg = RowDescription.parse(bytes, offset);
				break;
			case 'D':
				pg = DataRow.parse(bytes, offset);
				break;

			case 'I':
				pg = EmptyQueryResponse.parse(bytes, offset);
				break;

			case 'G':
				pg = CopyInResponse.parse(bytes, offset);
				break;
			case 'H':
				pg = CopyOutResponse.parse(bytes, offset);
				break;
			case '1':
				pg = ParseComplete.parse(bytes, offset);
				break;
			default:
				throw new RuntimeException("Unknown packet");
			}
			if (pg != null) {
				offset = offset + pg.getLength() + 1;
				pgs.add(pg);
			}
			
		}
		return pgs;
	}
	
	@Deprecated
	private static List<PostgreSQLPacket> parsePacket(byte[] bytes, int offset,
			int readLength) throws IOException {
		List<PostgreSQLPacket> pgs = new ArrayList<>();
		while (offset < readLength) {
			char MAKE = (char) bytes[offset];
			PostgreSQLPacket pg = null;
			switch (MAKE) {
			case 'R':
				pg = AuthenticationPacket.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'E':
				pg = ErrorResponse.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'K':
				pg = BackendKeyData.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'S':
				pg = ParameterStatus.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'Z':
				pg = ReadyForQuery.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'N':
				pg = NoticeResponse.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'C':
				pg = CommandComplete.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'T':
				pg = RowDescription.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'D':
				pg = DataRow.parse(ByteBuffer.wrap(bytes), offset);
				break;

			case 'I':
				pg = EmptyQueryResponse.parse(ByteBuffer.wrap(bytes), offset);
				break;

			case 'G':
				pg = CopyInResponse.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case 'H':
				pg = CopyOutResponse.parse(ByteBuffer.wrap(bytes), offset);
				break;
			case '1':
				pg = ParseComplete.parse(ByteBuffer.wrap(bytes), offset);
				break;
			default:
				throw new RuntimeException("Unknown packet");
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
	public static String createPostgresTimeZone() {
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

	public static ByteBuffer makeStartUpPacket(String user, String database)
			throws IOException {
		List<String[]> paramList = new ArrayList<String[]>();
		String appName = "MyCat-Server";
		paramList.add(new String[] { "user", user });
		paramList.add(new String[] { "database", database });
		paramList.add(new String[] { "client_encoding", "UTF8" });
		paramList.add(new String[] { "DateStyle", "ISO" });
		paramList.add(new String[] { "TimeZone", createPostgresTimeZone() });
		paramList.add(new String[] { "extra_float_digits", "3" });
		paramList.add(new String[] { "application_name", appName });
		String[][] params = paramList.toArray(new String[0][]);
		StringBuilder details = new StringBuilder();
		for (int i = 0; i < params.length; ++i) {
			if (i != 0) {
				details.append(", ");
			}
			details.append(params[i][0]);
			details.append("=");
			details.append(params[i][1]);
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
		return buffer;
	}

}
