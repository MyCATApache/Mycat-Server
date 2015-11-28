package io.mycat.backend.postgresql.utils;

import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.RowDescription;
import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.RowDataPacket;

import java.util.List;


/*********
 * 数据包适配
 * @author Coollf
 *
 */
public class PgPacketApaterUtils {
	/**
	 * 列标示转换成Mysql的数据
	 * @param description
	 * @return
	 */
	public static List<FieldPacket> rowDescConvertFieldPacket(RowDescription description){
		//TODO 等待实现
		return null;		
	}
	
	/***
	 * 行数据转换成mysql的数据
	 * @param dataRow
	 * @return
	 */
	public static RowDataPacket rowDataConvertRowDataPacket(DataRow dataRow){
		//TODO 等待实现
		return null;
	}
}
