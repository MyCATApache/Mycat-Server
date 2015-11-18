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

	public static List<PostgreSQLPacket> parsePacket(byte[] bytes, int offset, int readLength) throws IOException {
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

}
