package org.opencloudb.mpp.tmp;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

/**
 * 基于虚拟内存的固定长度数据项Array<br/>
 * 非HotSpotVM需要重写release函<br/>
 * 数释放文件句柄 <br/>
 * 
 * @author Czp
 *
 */

public class MemMapLongArray {

	private int size;
	private boolean isClose;
	private File swapFilePath;
	private MappedByteBuffer curBuf;
	private LinkedList<File> mapFiles;
	private ArrayList<MappedByteBuffer> bufs;
	private LinkedList<RandomAccessFile> foss;

	/**
	 * 每次扩容的大小,必须是long字节数的倍数
	 */
	private static final int INIT_SIZE = 8 * 30 * 1024 * 1024;

	public MemMapLongArray(String swapPath) {
		mapFiles = new LinkedList<File>();
		bufs = new ArrayList<MappedByteBuffer>();
		foss = new LinkedList<RandomAccessFile>();
		createSwapPath(swapPath);
		createBuffer();
	}

	public synchronized boolean add(long data) {
		int capacity = curBuf.capacity();
		int remain = 8 + curBuf.position();
		if (capacity == remain) {
			curBuf.putLong(data);
			createBuffer();
		} else if (capacity < remain) {
			createBuffer();
			curBuf.putLong(data);
		} else if (capacity > remain) {
			curBuf.putLong(data);
		}
		size++;
		return true;
	}

	public synchronized long get(int index) {
		MemMapUtil.checkRangeState(index, size, isClose);
		int pos = index * 8;// index << 3;
		int bufIndex = pos / INIT_SIZE;// pos >> MOD_NUM;
		MappedByteBuffer buf = bufs.get(bufIndex);
		int oldPos = buf.position();
		buf.position(pos % INIT_SIZE);// pos & ((1 << MOD_NUM) - 1)
		long data = buf.getLong();
		buf.position(oldPos);
		return data;
	}

	public synchronized long set(int index, long data) {
		MemMapUtil.checkRangeState(index, size, isClose);
		int pos = index * 8;
		int bufIndex = pos / INIT_SIZE;
		MappedByteBuffer buf = bufs.get(bufIndex);
		int oldPos = buf.position();
		buf.position(pos % INIT_SIZE);
		buf.putLong(data);
		buf.position(oldPos);
		return data;
	}

	public synchronized int size() {
		return size;
	}

	public synchronized void release() {
		isClose = true;
		for (RandomAccessFile fos : foss) {
			MemMapUtil.close(fos);
		}
		for (MappedByteBuffer mbuf : bufs) {
			MemMapUtil.releaseMemory(mbuf);
		}
		for (File f : mapFiles) {
			if (!f.delete())
				System.out.println("release mapbuf fail");
		}
		foss.clear();
		bufs.clear();
		mapFiles.clear();
	}

	private void createSwapPath(String swapPath) {
		swapFilePath = new File(swapPath);
		if (!swapFilePath.exists())
			swapFilePath.mkdirs();
	}

	private void createBuffer() {
		File file = null;
		RandomAccessFile mapFos = null;
		try {
			file = File.createTempFile("mapl-", ".dat", swapFilePath);
			mapFos = new RandomAccessFile(file, "rw");
			curBuf = mapFos.getChannel().map(MapMode.READ_WRITE, 0, INIT_SIZE);
			foss.add(mapFos);
			bufs.add(curBuf);
			mapFiles.add(file);
		} catch (Exception e) {
			MemMapUtil.close(mapFos);
			if (file != null)
				file.delete();
			throw new RuntimeException(e);
		}
	}

}