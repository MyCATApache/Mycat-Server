package io.mycat.backend.postgresql.packet;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

//ErrorResponse (B)
//Byte1('E')
//标识消息是一条错误。
//
//Int32
//以字节记的消息内容的长度，包括长度本身。
//
//消息体由一个或多个标识出来的字段组成，后面跟着一个字节零作为终止符。 字段可以以任何顺序出现。对于每个字段都有下面的东西：
//
//Byte1
//一个标识字段类型的代码；如果为零，这就是消息终止符并且不会跟着有字串。 目前定义的字段类型在 Section 43.5 列出。 因为将来可能增加更多的字段类型，所以前端应该不声不响地忽略不认识类型的字段。
//
//String
//字段值。

public class ErrorResponse extends PostgreSQLPacket {
	/*********
	 * 解析错误包
	 * 
	 * @param buffer
	 * @param offset
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws IllegalAccessException
	 */
	public static ErrorResponse parse(ByteBuffer buffer, int offset)
			throws IllegalArgumentException  {
		if ((char) buffer.get(offset) != PacketMarker.B_Error.getValue()) {
			throw new IllegalArgumentException("this packet not is ErrorResponse");
		}
		ErrorResponse err = new ErrorResponse();
		err.length = buffer.getInt(offset + 1);
		err.mark = buffer.get(offset + 1 + 4);
		if (err.mark != 0) {
			byte[] str = new byte[err.length - (4+4)];
			for(int i =0;i<str.length;i++){
				str[i] = buffer.get(offset + 1 + 4 + 4 +i);
			}
			err.errMsg = new String(str,UTF8);
		}
		return err;
	}

	private int length;

	private byte mark;

	private String errMsg;

	public String getErrMsg() {
		return errMsg;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PostgreSQLPacket.PacketMarker.B_Error.getValue();
	}

}
