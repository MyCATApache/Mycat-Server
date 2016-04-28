package io.mycat.backend.postgresql.packet;

import java.nio.charset.Charset;

public abstract class PostgreSQLPacket {

	public final static Charset UTF8 = Charset.forName("utf-8");

	/***
	 * 获取包长度
	 * 
	 * @return
	 */
	public abstract int getLength();

	/***
	 * 获取包标记
	 * 
	 * @return
	 */
	public abstract char getMarker();
	
	
	public int getPacketSize(){
		return getLength() + 1;
	}
	
	public String getType(){
		return this.getClass().getSimpleName();
	}
	
	/***
	 * 数据类型
	 * 
	 * @author Coollf
	 *
	 */
	public static enum DateType {
		bit_(1560), boo_(16), box_(603), bytea_(17), char_(1042), cidr_(650), circle_(
				718), date_(1082), decimal_(1700), float4_(700), float8_(701), inet_(
				869), int2_(21), int4_(23), int8_(20), interval_(1186), json_(
				114), jsonb_(3802), line_(628), lseg_(601), macaddr_(829), money_(
				790), path_(602), point_(600), polygon_(604), serial2_(21), serial4_(
				23), serial8_(20), text_(25), time_(1083), timetz_(1266), timestamp_(
				1114), timestamptz_(1184), tsquery_(3615), tsvector_(3614), txid_snapshot_(
				2970), uuid_(2950), varbit_(1562), varchar_(1043), xml_(142), UNKNOWN;

		private int value = 0;

		public static DateType valueOf(int val) {
			switch (val) {
			case 1560:
				return bit_;
			case 16:
				return boo_;
			case 603:
				return box_;
			case 17:
				return bytea_;
			case 1042:
				return char_;
			case 650:
				return cidr_;
			case 718:
				return circle_;
			case 1082:
				return date_;
			case 1700:
				return decimal_;
			case 700:
				return float4_;
			case 701:
				return float8_;
			case 869:
				return inet_;
			case 21:
				return int2_;
			case 23:
				return int4_;
			case 20:
				return int8_;
			case 1186:
				return interval_;
			case 114:
				return json_;
			case 3802:
				return jsonb_;
			case 628:
				return line_;
			case 601:
				return lseg_;
			case 829:
				return macaddr_;
			case 790:
				return money_;
			case 602:
				return path_;
			case 600:
				return point_;
			case 604:
				return polygon_;
//			case 21:
//				return serial2_;
//			case 23:
//				return serial4_;
//			case 20:
//				return serial8_;
			case 25:
				return text_;
			case 1083:
				return time_;
			case 1266:
				return timetz_;
			case 1114:
				return timestamp_;
			case 1184:
				return timestamptz_;
			case 3615:
				return tsquery_;
			case 3614:
				return tsvector_;
			case 2970:
				return txid_snapshot_;
			case 2950:
				return uuid_;
			case 1562:
				return varbit_;
			case 1043:
				return varchar_;
			case 142:
				return xml_;
			}
			return UNKNOWN;
		}

		private DateType() {
		}

		private DateType(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}

	}

	/****
	 * 数据协议
	 * 
	 * @author Coollf
	 *
	 */
	public static enum DataProtocol {
		TEXT, BINARY, UNKNOWN;

		public static DataProtocol valueOf(short val) {
			if (val == 0) {
				return TEXT;
			}
			if (val == 1) {
				return BINARY;
			}
			return UNKNOWN;
		}
	}

	public static enum PacketMarker {
		/**
		 * 认证包
		 */
		B_Auth('R'),

		/***
		 * 密码请求包
		 */
		F_PwdMess('p'),

		/**
		 * 错误包响应
		 */
		B_Error('E'),

		/***
		 * 后台传回的秘钥
		 */
		B_BackendKey('K'),

		/***
		 * paramter 状态信息
		 */
		B_ParameterStatus('S'),

		/**
		 * 等待查询
		 */
		B_ReadyForQuery('Z'),

		/**
		 * 警告响应
		 */
		B_NoticeResponse('N'),

		/***
		 * 简单查询
		 */
		F_Query('Q'),

		/******
		 * SQL 命令正常结束
		 */
		B_CommandComplete('C'),

		/***
		 * 数据行描述
		 */
		B_RowDescription('T'),

		/***
		 * 数据行数据
		 */
		B_DataRow('D'),

		/***
		 * 空查询
		 */
		B_EmptyQueryResponse('I'),

		/*************
		 * 拷贝数据进PGsql
		 */
		B_CopyInResponse('G'),

		/***
		 * 从PGsql 中拷贝数据出来
		 */
		B_CopyOutResponse('H'),

		/**
		 * 连接启动信息
		 */
		F_StartupMessage('\0'),

		/***
		 * 终止请求
		 */
		F_Terminate('X'),

		/***
		 * 解析sql语句请求
		 */
		F_Parse('P'),

		/**
		 * sql 解析成功
		 */
		B_ParseComplete('1'),

		/***
		 * 绑定参数成功
		 */
		B_BindComplete('2');

		private char value;

		private PacketMarker(char marker) {
			this.value = marker;
		}

		public char getValue() {
			return value;
		}
	}

}
