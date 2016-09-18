package io.mycat.backend.postgresql.packet;
//		NotificationResponse (B)
//		Byte1('A')
//		标识这条消息是一个通知响应。
//		
//		Int32
//		以字节记地消息内容地长度，包括长度本身。
//		
//		Int32
//		通知后端进程地进程 ID。
//		
//		String
//		触发通知的条件的名字。
//		
//		String
//		从通知进程传递过来的额外的信息。（目前，这个特性还未实现，因此这个字段总是一个空字串。）
public class NotificationResponse extends PostgreSQLPacket{

	@Override
	public int getLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char getMarker() {
		// TODO Auto-generated method stub
		return 0;
	}

}
