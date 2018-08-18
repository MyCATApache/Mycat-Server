package io.mycat.backend.postgresql.packet;

import io.mycat.backend.postgresql.utils.PIOUtils;

import java.nio.ByteBuffer;

//		CancelRequest (F)
//		Int32(16)
//		以字节计的消息长度。包括长度本身。
//		
//		Int32(80877102)
//		取消请求代码。选这个值是为了在高16位包含 1234， 低16位包含 5678。（为避免混乱，这个代码必须与协议版本号不同．）
//		
//		Int32
//		目标后端的进程号（PID）。
//		
//		Int32
//		目标后端的密钥（secret key）。

/***
 * 取消请求
 * 
 * @author Coollf
 *
 */
public class CancelRequest extends PostgreSQLPacket {
	private int length = 16;
	private int cancelCode = 80877102;
	private int pid;
	private int secretKey;

	public CancelRequest(int pid, int secretKey) {
		this.pid = pid;
		this.secretKey = secretKey;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return 0;
	}

	public void write(ByteBuffer buffer) {
		PIOUtils.SendInteger4(length, buffer);
		PIOUtils.SendInteger4(cancelCode, buffer);
		PIOUtils.SendInteger4(pid, buffer);
		PIOUtils.SendInteger4(secretKey, buffer);
	}

}
