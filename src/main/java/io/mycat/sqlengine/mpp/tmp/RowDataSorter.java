package io.mycat.sqlengine.mpp.tmp;

import io.mycat.net.mysql.RowDataPacket;
import io.mycat.sqlengine.mpp.OrderCol;
import io.mycat.sqlengine.mpp.RowDataPacketSorter;

import java.util.Collections;
import java.util.List;

/**
 * 
 * @author coderczp-2014-12-8
 */
public class RowDataSorter extends RowDataPacketSorter {

	// 记录总数(=offset+limit)
	private volatile int total;
	// 查询的记录数(=limit)
	private volatile int size;
	// 堆
	private volatile HeapItf heap;
	// 多列比较器
	private volatile RowDataCmp cmp;
	// 是否执行过buildHeap
	private volatile boolean hasBuild;

	public RowDataSorter(OrderCol[] orderCols) {
		super(orderCols);
		this.cmp = new RowDataCmp(orderCols);
	}

	public synchronized void setLimit(int start, int size) {
		// 容错处理
		if (start < 0) {
			start = 0;
		}
		if (size <= 0) {
			this.total = this.size = Integer.MAX_VALUE;
		} else {
			this.total = start + size;
			this.size = size;
		}
		// 统一采用顺序，order by 条件交给比较器去处理
		this.heap = new MaxHeap(cmp, total);
	}

	@Override
	public synchronized boolean addRow(RowDataPacket row) {
		if (heap.getData().size() < total) {
			heap.add(row);
			return true;
		}
		// 堆已满，构建最大堆，并执行淘汰元素逻辑
		if (heap.getData().size() == total && hasBuild == false) {
			heap.buildHeap();
			hasBuild = true;
		}
		return heap.addIfRequired(row);
	}

	@Override
	public List<RowDataPacket> getSortedResult() {
		final List<RowDataPacket> data = heap.getData();
		if (data.size() < 2) {
			return data;
		}
		
		if (total - size > data.size()) {
			return Collections.emptyList();
		}

		// 构建最大堆并排序
		if (!hasBuild) {
			heap.buildHeap();
		}
		heap.heapSort(this.size);
		return heap.getData();
	}

	public RowDataCmp getCmp() {
		return cmp;
	}

}
