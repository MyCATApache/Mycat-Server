package io.mycat.backend.postgresql.packet;

public abstract class PostgreSQLPacket {

	/***
	 * 获取包长度
	 * @return
	 */
	public abstract int getLength();
	
	
	/***
	 * 获取包标记
	 * @return
	 */
	public abstract char getMarker();
	
	
	public static enum PacketMarker{
		/**
		 * 认证包
		 */
		B_Auth('R'),
		
		/***
		 * 密码请求包
		 */
		F_PwdMess('p')
		;
		
		private char value;

		private PacketMarker(char marker) {
			this.value = marker;
		}

		public char getValue() {
			return value;
		}
	}

}
