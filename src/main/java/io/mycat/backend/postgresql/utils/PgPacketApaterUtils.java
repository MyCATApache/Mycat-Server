package io.mycat.backend.postgresql.utils;

import io.mycat.backend.postgresql.packet.DataRow;
import io.mycat.backend.postgresql.packet.DataRow.DataColumn;
import io.mycat.backend.postgresql.packet.PostgreSQLPacket.DateType;
import io.mycat.backend.postgresql.packet.RowDescription;
import io.mycat.backend.postgresql.packet.RowDescription.ColumnDescription;
import io.mycat.config.Fields;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


/*********
 * 数据包适配
 * @author Coollf
 *
 */
public class PgPacketApaterUtils {
	private static final Charset UTF8 = Charset.forName("utf-8");

	/**
	 * 列标示转换成Mysql的数据
	 * @param description
	 * @return
	 */
	public static List<FieldPacket> rowDescConvertFieldPacket(RowDescription description){
		List<FieldPacket>  fieldPks = new ArrayList<FieldPacket>(description.getColumnNumber());
		for(ColumnDescription c: description.getColumns()){
			FieldPacket fieldPk = new FieldPacket();
			fieldPk.name = c.getColumnName().trim().getBytes(UTF8);
			fieldPk.type = convertFieldType(c.getColumnType());
			fieldPks.add(fieldPk);
		}
		//TODO 等待实现
		return fieldPks;		
	}
	
	/***
	 * 将pg的sql类型转换成
	 * @param columnType
	 * @return
	 */
	private static int convertFieldType(DateType columnType) {
		if(columnType == DateType.timestamp_){
			return Fields.FIELD_TYPE_TIMESTAMP;
		}
		if(columnType == DateType.int2_ || columnType == DateType.int4_ || columnType == DateType.int8_ ){
			return Fields.FIELD_TYPE_INT24;
		}
		if(columnType == DateType.decimal_){
			return Fields.FIELD_TYPE_NEW_DECIMAL;
		}		
		if(columnType == DateType.UNKNOWN){
		
		}
		return Fields.FIELD_TYPE_VARCHAR;
	}


	/***
	 * 行数据转换成mysql的数据
	 * @param dataRow
	 * @return
	 */
	public static RowDataPacket rowDataConvertRowDataPacket(DataRow dataRow){
		RowDataPacket curRow = new RowDataPacket(dataRow.getColumnNumber());
		for(DataColumn c: dataRow.getColumns()){
			curRow.add(c.getData());
		}		
		return curRow;
	}
}
