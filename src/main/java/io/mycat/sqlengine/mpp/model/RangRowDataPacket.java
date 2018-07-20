package io.mycat.sqlengine.mpp.model;

import io.mycat.net.mysql.RowDataPacket;

import java.util.ArrayList;
import java.util.List;

public class RangRowDataPacket {
	public static final int DATA_TYPE_ALL = 100;
	public static final int DATA_TYPE_TRIM = 200;
	private int dataType = DATA_TYPE_ALL;
	private List<RowDataPacket> rowDataPacketList = new ArrayList<RowDataPacket>();
	
	private int trimCount = 0;
	public int getTrimCount() {
		return trimCount;
	}

	public void appendPacket(List<RowDataPacket> packetList) {
		this.rowDataPacketList.addAll(packetList);
	}
	
	public void appendPacket(RowDataPacket packet) {
		this.rowDataPacketList.add(packet);
	}
	
	public boolean isTrim() {
		return dataType == DATA_TYPE_TRIM ? true : false;
	}
	
	public void leftHeadTail() {
		while (this.rowDataPacketList.size() > 2) {
			this.rowDataPacketList.remove(1);
			trimCount++;
		}
		dataType = DATA_TYPE_TRIM;
	}
	
	public int allSize() {
		if (dataType == DATA_TYPE_TRIM) {
			return trimCount + rowDataPacketList.size();
		} else {
			return rowDataPacketList.size();
		}
	}
	
	public void combine(RangRowDataPacket rowData) {
		if (dataType == DATA_TYPE_TRIM) {
			if (this.rowDataPacketList.isEmpty()) {
				this.trimCount = rowData.getTrimCount();
				this.rowDataPacketList.addAll(rowData.getRowDataPacketList());
			} else {
				if (rowData.allSize() == 0) {
					return;
				}
				this.rowDataPacketList.remove(1);
				
				if (rowData.allSize() == 1) {
					this.trimCount += 1 + rowData.getTrimCount();
					this.rowDataPacketList.add(rowData.getHead());
				} else if (rowData.allSize() >= 2) {
					this.trimCount += 2 + rowData.getTrimCount();
					this.rowDataPacketList.add(rowData.getTail());
				}
			}
		}
		
	}
	
	public List<RowDataPacket> getRowDataPacketList() {
		return rowDataPacketList;
	}
	
	public RowDataPacket getHead() {
		if (this.rowDataPacketList.isEmpty()) {
			return null;
		}
		return this.rowDataPacketList.get(0);
	}
	
	public RowDataPacket getTail() {
		if (this.rowDataPacketList.size() < 2) {
			return null;
		}
		return this.rowDataPacketList.get(this.rowDataPacketList.size()-1);
	}
}
