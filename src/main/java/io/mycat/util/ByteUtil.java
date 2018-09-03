/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.util;

import java.nio.charset.Charset;
import java.util.Date;

public class ByteUtil {

	/**
	 * compare to number or dicamal ascii byte array, for number :123456 ,store
	 * to array [1,2,3,4,5,6]
	 * 
	 * @param b1
	 * @param b2
	 * @return -1 means b1 < b2, or 0 means b1=b2 else return 1
	 */
	public static int compareNumberByte(byte[] b1, byte[] b2) {
		if(b1 == null || b1.length == 0) {
			return -1;
		}
		else if(b2 == null || b2.length == 0) {
			return 1;
		}
		boolean isNegetive = b1[0] == 45 || b2[0] == 45;
		if (isNegetive == false && b1.length != b2.length) {
			return b1.length - b2.length;
		}
		int len = b1.length > b2.length ? b2.length : b1.length;
		int result = 0;
		int index = -1;
		for (int i = 0; i < len; i++) {
			int b1val = b1[i];
			int b2val = b2[i];
			if (b1val > b2val) {
				result = 1;
				index = i;
				break;
			} else if (b1val < b2val) {
				index = i;
				result = -1;
				break;
			}
		}
		if (index == 0) {
			// first byte compare
			return result;
		} else {
            if( b1.length != b2.length ) {

                int lenDelta = b1.length - b2.length;
                return isNegetive ? 0 - lenDelta : lenDelta;

            } else {
                return isNegetive ? 0 - result : result;
            }
		}
	}

	public static byte[] compareNumberArray2(byte[] b1, byte[] b2, int order) {
		if (b1.length <= 0 && b2.length > 0) {
			return b2;
		}
		if (b1.length > 0 && b2.length <= 0) {
			return b1;
		}
		int len = b1.length > b2.length ? b1.length : b2.length;
		for (int i = 0; i < len; i++) {
			if (b1[i] != b2[i]) {
				if (order == 1) {
					return ((b1[i] & 0xff) - (b2[i] & 0xff)) > 0 ? b1 : b2;
				} else {
					return ((b1[i] & 0xff) - (b2[i] & 0xff)) > 0 ? b2 : b1;
				}
			}
		}

		return b1;
	}

	public static byte[] getBytes(short data) {
		byte[] bytes = new byte[2];
		bytes[0] = (byte) (data & 0xff);
		bytes[1] = (byte) ((data & 0xff00) >> 8);
		return bytes;
	}

	public static byte[] getBytes(char data) {
		byte[] bytes = new byte[2];
		bytes[0] = (byte) (data);
		bytes[1] = (byte) (data >> 8);
		return bytes;
	}

	public static byte[] getBytes(int data) {
		byte[] bytes = new byte[4];
		bytes[0] = (byte) (data & 0xff);
		bytes[1] = (byte) ((data & 0xff00) >> 8);
		bytes[2] = (byte) ((data & 0xff0000) >> 16);
		bytes[3] = (byte) ((data & 0xff000000) >> 24);
		return bytes;
	}

	public static byte[] getBytes(long data) {
		byte[] bytes = new byte[8];
		bytes[0] = (byte) (data & 0xff);
		bytes[1] = (byte) ((data >> 8) & 0xff);
		bytes[2] = (byte) ((data >> 16) & 0xff);
		bytes[3] = (byte) ((data >> 24) & 0xff);
		bytes[4] = (byte) ((data >> 32) & 0xff);
		bytes[5] = (byte) ((data >> 40) & 0xff);
		bytes[6] = (byte) ((data >> 48) & 0xff);
		bytes[7] = (byte) ((data >> 56) & 0xff);
		return bytes;
	}

	public static byte[] getBytes(float data) {
		int intBits = Float.floatToIntBits(data);
		return getBytes(intBits);
	}

	public static byte[] getBytes(double data) {
		long intBits = Double.doubleToLongBits(data);
		return getBytes(intBits);
	}

	public static byte[] getBytes(String data, String charsetName) {
		Charset charset = Charset.forName(charsetName);
		return data.getBytes(charset);
	}

	public static byte[] getBytes(String data) {
		return getBytes(data, "GBK");
	}

	public static short getShort(byte[] bytes) {
		return Short.parseShort(new String(bytes));
//		return (short) ((0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)));
	}

	public static char getChar(byte[] bytes) {
		return (char) ((0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)));
	}

	public static int getInt(byte[] bytes) {
		return Integer.parseInt(new String(bytes));
		// return (0xff & bytes[0]) | (0xff00 & (bytes[1] << 8)) | (0xff0000 &
		// (bytes[2] << 16)) | (0xff000000 & (bytes[3] << 24));
	}

	public static long getLong(byte[] bytes) {
		return Long.parseLong(new String(bytes));
		// return(0xffL & (long)bytes[0]) | (0xff00L & ((long)bytes[1] << 8)) |
		// (0xff0000L & ((long)bytes[2] << 16)) | (0xff000000L & ((long)bytes[3]
		// << 24))
		// | (0xff00000000L & ((long)bytes[4] << 32)) | (0xff0000000000L &
		// ((long)bytes[5] << 40)) | (0xff000000000000L & ((long)bytes[6] <<
		// 48)) | (0xff00000000000000L & ((long)bytes[7] << 56));
	}

	public static double getDouble(byte[] bytes) {
		return Double.parseDouble(new String(bytes));
	}
	
	public static float getFloat(byte[] bytes) {
		return Float.parseFloat(new String(bytes));
	}

	public static String getString(byte[] bytes, String charsetName) {
		return new String(bytes, Charset.forName(charsetName));
	}

	public static String getString(byte[] bytes) {
		return getString(bytes, "UTF-8");
	}

	public static String getDate(byte[] bytes) {
		return new String(bytes);
	}
	
	public static String getTime(byte[] bytes) {
		return new String(bytes);
	}

	public static String getTimestmap(byte[] bytes) {
		return new String(bytes);
	}
	
	public static byte[] getBytes(Date date, boolean isTime) {
		if(isTime) {
			return getBytesFromTime(date);
		} else {
			return getBytesFromDate(date);
		}
    }
	
	private static byte[] getBytesFromTime(Date date) {
		int day = 0;
		int hour = DateUtil.getHour(date);
		int minute = DateUtil.getMinute(date);
    	int second = DateUtil.getSecond(date);
    	int microSecond = DateUtil.getMicroSecond(date);
    	byte[] bytes = null;
    	byte[] tmp = null;
    	if(day == 0 && hour == 0 && minute == 0
    			&& second == 0 && microSecond == 0) {
    		bytes = new byte[1];
    		bytes[0] = (byte) 0;
    	} else if(microSecond == 0) {
    		bytes = new byte[1 + 8];
    		bytes[0] = (byte) 8;
    		bytes[1] = (byte) 0; // is_negative (1) -- (1 if minus, 0 for plus)
    		tmp = getBytes(day);
    		bytes[2] = tmp[0];
    		bytes[3] = tmp[1];
    		bytes[4] = tmp[2];
    		bytes[5] = tmp[3];
    		bytes[6] = (byte) hour;
    		bytes[7] = (byte) minute;
    		bytes[8] = (byte) second;
    	} else {
    		bytes = new byte[1 + 12];
    		bytes[0] = (byte) 12;
    		bytes[1] = (byte) 0; // is_negative (1) -- (1 if minus, 0 for plus)
    		tmp = getBytes(day);
    		bytes[2] = tmp[0];
    		bytes[3] = tmp[1];
    		bytes[4] = tmp[2];
    		bytes[5] = tmp[3];
    		bytes[6] = (byte) hour;
    		bytes[7] = (byte) minute;
    		bytes[8] = (byte) second;
    		tmp = getBytes(microSecond);
    		bytes[9] = tmp[0];
    		bytes[10] = tmp[1];
    		bytes[11] = tmp[2];
    		bytes[12] = tmp[3];
    	}
    	return bytes;
	}
	
	private static byte[] getBytesFromDate(Date date) {
		int year = DateUtil.getYear(date);
    	int month = DateUtil.getMonth(date);
    	int day = DateUtil.getDay(date);
    	int hour = DateUtil.getHour(date);
    	int minute = DateUtil.getMinute(date);
    	int second = DateUtil.getSecond(date);
    	int microSecond = DateUtil.getMicroSecond(date);
    	byte[] bytes = null;
    	byte[] tmp = null;
    	if(year == 0 && month == 0 && day == 0 
    			&& hour == 0 && minute == 0 && second == 0
    			&& microSecond == 0) {
    		bytes = new byte[1];
    		bytes[0] = (byte) 0;
    	} else if(hour == 0 && minute == 0 && second == 0
    			&& microSecond == 0) {
    		bytes = new byte[1 + 4];
    		bytes[0] = (byte) 4;
    		tmp = getBytes((short) year);
    		bytes[1] = tmp[0];
    		bytes[2] = tmp[1];
    		bytes[3] = (byte) month;
    		bytes[4] = (byte) day;
    	} else if(microSecond == 0) {
    		bytes = new byte[1 + 7];
    		bytes[0] = (byte) 7;
    		tmp = getBytes((short) year);
    		bytes[1] = tmp[0];
    		bytes[2] = tmp[1];
    		bytes[3] = (byte) month;
    		bytes[4] = (byte) day;
    		bytes[5] = (byte) hour;
    		bytes[6] = (byte) minute;
    		bytes[7] = (byte) second;
    	} else {
    		bytes = new byte[1 + 11];
    		bytes[0] = (byte) 11;
    		tmp = getBytes((short) year);
    		bytes[1] = tmp[0];
    		bytes[2] = tmp[1];
    		bytes[3] = (byte) month;
    		bytes[4] = (byte) day;
    		bytes[5] = (byte) hour;
    		bytes[6] = (byte) minute;
    		bytes[7] = (byte) second;
    		tmp = getBytes(microSecond);
    		bytes[8] = tmp[0];
    		bytes[9] = tmp[1];
    		bytes[10] = tmp[2];
    		bytes[11] = tmp[3];
    	}
    	return bytes;
	}
	
	// 支持 byte dump
	//---------------------------------------------------------------------
	public static String dump(byte[] data, int offset, int length) {
		
		StringBuilder sb = new StringBuilder();
		sb.append(" byte dump log ");
		sb.append(System.lineSeparator());
		sb.append(" offset ").append( offset );
		sb.append(" length ").append( length );
		sb.append(System.lineSeparator());
		int lines = (length - 1) / 16 + 1;
		for (int i = 0, pos = 0; i < lines; i++, pos += 16) {
			sb.append(String.format("0x%04X ", i * 16));
			for (int j = 0, pos1 = pos; j < 16; j++, pos1++) {
				sb.append(pos1 < length ? String.format("%02X ", data[offset + pos1]) : "   ");
			}
			sb.append(" ");
			for (int j = 0, pos1 = pos; j < 16; j++, pos1++) {
				sb.append(pos1 < length ? print(data[offset + pos1]) : '.');
			}
			sb.append(System.lineSeparator());
		}
		sb.append(length).append(" bytes").append(System.lineSeparator());
		return sb.toString();
	}

	public static char print(byte b) {
		return (b < 32 || b > 127) ? '.' : (char) b;
	}

	/*
	 * 返回小数点的位置
	 * @return 找不到 ,其他都是小数点的位置
	 * */
	public static int getDot(byte[] array) {		
		for(int s = 0; s < array.length; s++){
		   if(array[s] == 46) {
			   return s;
		   }
		}
		return array.length;
	}
	/*
	 * @return 返回是否是科学计数法
	 * */
	public static boolean hasE(byte[] array) {		
		for(int s = 0; s < array.length; s++){
		   if(array[s] == 'E' || array[s] == 'e') {
			   return true;
		   }
		}
		return false;
	}
	/*
	 * @比較        對於b1取0 到b1End進行比較
	 *          對於b2取0 到b2End進行比較
	 * */
	public static int compareNumberByte(byte[] b1, int b1End, byte[] b2, int b2End) {
		if(b1 == null || b1.length == 0) {
			return -1;
		}
		else if(b2 == null || b2.length == 0) {
			return 1;
		}
		boolean isNegetive = b1[0] == 45 || b2[0] == 45; // 45 表示负数符号

		//正数 长度不等 直接返回 长度 		//都是正数长度不等,直接返回长度的比较
		if (isNegetive == false && b1End != b2End) {
			return b1End - b2End;
		}
		//取短的一个
		int len = b1End > b2End ? b2End : b1End;
		int result = 0;
		int index = -1;
		for (int i = 0; i < len; i++) {
			int b1val = b1[i];
			int b2val = b2[i];
			if (b1val > b2val) {
				result = 1;
				index = i;
				break;
			} else if (b1val < b2val) {
				index = i;
				result = -1;
				break;
			}
		}		
		if (index == 0) {
			//正负数直接符号比较
			// first byte compare ,数值与符号的比较 一正 一负数
			return result;
		} else {
            if( b1End != b2End ) {
            	//都是正数 长度不等, 都是负数 长度不等
                int lenDelta = b1End - b2End;
                return isNegetive ? 0 - lenDelta : lenDelta;

            } else {
            	//长度相等 符号相同
            	//位数相同 直接取比较结果的 正数就是结果,否则就是比较结果取反
                return isNegetive ? 0 - result : result;
            }
		}
	}
	/*
	 * double類型的b1 b2進行比較
	 * 首先:判斷是否是科學計數法 是直接創建Double對象進行比較
	 *      否則利用byte進行比較 
	 *         先判斷整數位 再判斷小數位
	 * */
	public static int compareDouble(byte[] b1, byte[] b2) {
		if(b1 == null || b1.length == 0) {
			return -1;
		}
		else if(b2 == null || b2.length == 0) {
			return 1;
		}
		boolean isHasE = hasE(b1) || hasE(b2);
		if(isHasE){			
			return Double.valueOf(new String(b1)).compareTo( Double.valueOf(new String(b2))) ;
		}
		int d1 = getDot(b1);
		int d2 = getDot(b2);
		//判斷整數位
		int result = compareNumberByte(b1, d1, b2, d2);
		//符号相等
		if(result == 0){
			//判斷小數位
			boolean isNegetive = b1[0] == 45 || b2[0] == 45; // 45 表示负数符号
			int xsLen1 = b1.length - d1;
			int xsLen2 = b2.length - d2;
			int len = xsLen1 > xsLen2 ? xsLen2 : xsLen1; //小数位数中的 小数
			int temp = 0;
			for(int i = 0; i < len ; i++) {
				temp = b1[i + d1] - b2 [i+ d2];
				if(temp != 0){
					break;
				}
			}
			if(temp == 0){
				//0.12 0.123 或者 -0.12 -0.123
				int lenDelta = xsLen1 - xsLen2;
                result = isNegetive? 0 - lenDelta : lenDelta;
			} else{
				//0.12 0.113 或者 -0.12 -0.113
				result = isNegetive? 0 - temp : temp;
			}
		}
		return result;
	}


}