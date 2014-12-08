package org.opencloudb.mpp.tmp;

import java.io.Closeable;
import java.nio.MappedByteBuffer;

public class MemMapUtil {

	/**
	 * 检测是否数组越界<br>
	 * 映射是否已经关闭<br>
	 * 
	 */
	public final static void checkRangeState(int index, int size,
			boolean isClose) {
		checkState(isClose);
		if (index >= size) {
			String format = "index over flow size:%s,index:%s";
			String msg = String.format(format, size, index);
			throw new RuntimeException(msg);
		}

	}

	/**
	 * 检测是否数组越界<br>
	 * 映射是否已经关闭<br>
	 * 
	 */
	public final static void checkRangeState(int index, int index2, int size,
			boolean isClose) {
		checkState(isClose);
		if (index >= size || index2 >= size) {
			String format = "index over flow size:%s,index:%s";
			String msg = String.format(format, size, index);
			throw new RuntimeException(msg);
		}

	}

	/**
	 * 关闭资源
	 * 
	 * @param ios
	 */
	public static void close(Closeable... ios) {
		for (Closeable closeable : ios) {
			try {
				if (closeable != null)
					closeable.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void releaseMemory(MappedByteBuffer mbuf) {
		((sun.nio.ch.DirectBuffer) mbuf).cleaner().clean();
	}

	public static void checkState(boolean isClose) {
		if (isClose) {
			throw new RuntimeException("memory map is destory");
		}
	}

	public static void checkDataLen(int length) {
		if (length > 65535)
			throw new RuntimeException("length must be less than 65535");
	}

	public static void checkStateAndLen(boolean isClose, int length) {
		checkState(isClose);
		checkDataLen(length);
	}

}
