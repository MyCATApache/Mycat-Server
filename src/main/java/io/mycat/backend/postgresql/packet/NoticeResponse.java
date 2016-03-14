package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

public class NoticeResponse extends PostgreSQLPacket {
	/**
	 * 长度
	 */
	int length;

	/**
	 * 标记
	 */
	byte mark;

	private String msg;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_NoticeResponse.getValue();
	}

	public static NoticeResponse parse(ByteBuffer buffer, int offset) {
		if (PacketMarker.B_NoticeResponse.getValue() != (char) buffer.get(offset)) {
			throw new IllegalArgumentException("this packet not is NoticeResponse");
		}
		NoticeResponse noticeResponse = new NoticeResponse();
		noticeResponse.length = buffer.getInt(offset + 1);
		noticeResponse.mark = buffer.get(offset + 1 + 4);
		if (noticeResponse.mark != 0) {
			byte[] str = new byte[noticeResponse.length - 4 - 1];
			for (int i = 0; i < str.length; i++) {
				str[i] = buffer.get(offset + 1 + 4 + 1 + i);
			}
			noticeResponse.msg = new String(str,UTF8);
		}
		return noticeResponse;
	}

	public String getMsg() {
		return msg;
	}

}
