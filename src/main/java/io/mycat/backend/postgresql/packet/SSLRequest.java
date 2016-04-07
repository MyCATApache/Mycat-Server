package io.mycat.backend.postgresql.packet;

//		SSLRequest (F)
//		Int32(8)
//		以字节记的消息内容的长度，包括长度本身。
//		
//		Int32(80877103)
//		SSL 请求码。选取的数值在高16位里包含 1234，在低16位里包含 5679。 （为了避免混淆，这个编码必须和任何协议版本号不同。）
public class SSLRequest {

}
