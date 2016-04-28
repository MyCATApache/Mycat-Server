package io.mycat.backend.postgresql.packet;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.packet.AuthenticationPacket.AuthType;
import io.mycat.backend.postgresql.utils.MD5Digest;
import io.mycat.backend.postgresql.utils.PIOUtils;

//PasswordMessage (F)
//Byte1('p')
//标识这条消息是一个口令响应。
//
//Int32
//以字节记的消息内容的长度，包括长度本身。
//
//String
//口令（如果要求了，就是加密后的）。
public class PasswordMessage extends PostgreSQLPacket {

	public PasswordMessage(String user, String password, AuthType aut, byte[] salt)
			throws UnsupportedEncodingException {
		if (aut == AuthType.MD5Password) {
			this.password = MD5Digest.encode(user.getBytes(UTF8), password.getBytes(UTF8), salt);
		}
	}

	private char marker = PacketMarker.F_PwdMess.getValue();

	private byte[] password;

	@Override
	public int getLength() {
		return  4 + password.length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public void write(ByteBuffer buffer) {
		PIOUtils.SendChar(getMarker(), buffer);
		PIOUtils.SendInteger4(getLength(), buffer);
		PIOUtils.Send(password, buffer);
	}

}
