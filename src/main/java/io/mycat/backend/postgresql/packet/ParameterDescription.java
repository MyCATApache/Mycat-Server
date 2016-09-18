package io.mycat.backend.postgresql.packet;
//		ParameterDescription (B)
//		Byte1('t')
//		标识消息是一个参数描述。
//		
//		Int32
//		以字节记的消息内容长度，包括长度本身。
//		
//		Int16
//		语句所使用的参数的个数（可以为零）。
//		
//		然后，对每个参数，有下面的东西。
//		
//		Int32
//		声明参数数据类型的对象 ID。
public class ParameterDescription {

}
