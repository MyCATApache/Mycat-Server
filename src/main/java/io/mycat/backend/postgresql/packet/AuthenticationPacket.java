package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

public class AuthenticationPacket extends PostgreSQLPacket {
	public static enum AuthType {
		Ok(0), KerberosV5(2), CleartextPassword(3), CryptPassword(4), MD5Password(5), SCMCredential(6);

		private int value;

		AuthType(int v) {
			this.value = v;
		}

		public int getValue() {
			return value;
		}

		public static AuthType valueOf(int v) {
			if (v == Ok.value) {
				return Ok;
			}
			if (v == KerberosV5.value) {
				return KerberosV5;
			}
			if (v == CleartextPassword.value) {
				return CleartextPassword;
			}
			if (v == MD5Password.value) {
				return MD5Password;
			}
			if (v == SCMCredential.value) {
				return SCMCredential;
			}

			return null;
		}
	}

	/***
	 * 标记
	 */
	private char marker = PacketMarker.B_Auth.getValue();

	/****
	 * 数据包长度
	 */
	private int length;

	/***
	 * 盐粒
	 */
	private byte[] salt;

	private AuthType authType;

	public AuthType getAuthType() {
		return authType;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public byte[] getSalt() {
		return salt;
	}

	public void setSalt(byte[] salt) {
		this.salt = salt;
	}

	public static AuthenticationPacket parse(ByteBuffer buffer, int offset){
		if (buffer.get(offset) != PacketMarker.B_Auth.getValue()) {
			throw new IllegalArgumentException("this packetData not is AuthenticationPacket");
		}
		AuthenticationPacket packet = new AuthenticationPacket();
		packet.length = PIOUtils.redInteger4(buffer, offset + 1);
		packet.authType = AuthType.valueOf(PIOUtils.redInteger4(buffer, offset + 1 + 4));
		if (packet.authType == AuthType.MD5Password) {
			packet.salt = PIOUtils.redByteArray(buffer, offset + 1 + 4 + 4, 4);
		}
		return packet;
	}
}
