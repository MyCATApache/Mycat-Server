package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

/***
 * 等待查询包
 * 
 * @author Coollf
 *
 */

// ReadyForQuery (B)
// Byte1('Z')
// 标识消息类型。在后端为新的查询循环准备好的时候， 总会发送 ReadyForQuery。
//
// Int32(5)
// 以字节记的消息内容的长度，包括长度本身。
//
// Byte1
// 当前后端事务状态指示器。可能的值是空闲状况下的 'I'（不在事务块里）；在事务块里是 'T'； 或者在一个失败的事务块里是
// 'E'（在事务块结束之前，任何查询都将被拒绝）。
public class ReadyForQuery extends PostgreSQLPacket {

	/*****
	 * 消息长度
	 */
	private int length;

	/***
	 * 状态
	 */
	TransactionState state;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_ReadyForQuery.getValue();
	}

	public static ReadyForQuery parse(ByteBuffer buffer, int offset) {
		if (buffer.get(offset) != PacketMarker.B_ReadyForQuery.getValue()) {
			throw new IllegalArgumentException("this packet not is ReadyForQuery");
		}
		ReadyForQuery readyForQuery = new ReadyForQuery();
		readyForQuery.length = buffer.getInt(offset + 1);
		readyForQuery.state = TransactionState.valueOf((char)buffer.get(offset+1+4));
		return readyForQuery;
	}

	/***
	 * 后端事物状态
	 * 
	 * @author Coollf
	 *
	 */
	public static enum TransactionState {
		/***
		 * 不在事物中
		 */
		NOT_IN('I'),

		/**
		 * 在事物中
		 */
		IN('T'),

		/***
		 * 错误
		 */
		ERR('E');

		private char vlaue;

		public char getVlaue() {
			return vlaue;
		}

		TransactionState(char value) {
			this.vlaue = value;
		}

		public static TransactionState valueOf(char v) {
			if (v == NOT_IN.getVlaue()) {
				return NOT_IN;
			}
			if (v == IN.getVlaue()) {
				return IN;
			}
			if (v == ERR.getVlaue()) {
				return TransactionState.ERR;
			}
			return null;
		}
	}

	/**
	 * @return the state
	 */
	public TransactionState getState() {
		return state;
	}

	/**
	 * @param state the state to set
	 */
	public void setState(TransactionState state) {
		this.state = state;
	}

}
