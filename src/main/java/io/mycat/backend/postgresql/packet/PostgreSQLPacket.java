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

	/***
	 * 数据类型
	 * 
	 * @author Coollf
	 *
	 */
	public static enum DateType {
		UNKNOWN;

		private int value = 0;

		public static DateType valueOf(int val) {
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
