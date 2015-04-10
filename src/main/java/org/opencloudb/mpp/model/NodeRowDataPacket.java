package org.opencloudb.mpp.model;

import java.util.ArrayList;
import java.util.List;

import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;

public class NodeRowDataPacket {

	private RouteResultsetNode node;
	private long trimTotal = 0;
	
	private int trimSize = 0;

	private List<RangRowDataPacket> trimRangRDPacketList = new ArrayList<RangRowDataPacket>();
	private List<RangRowDataPacket> rangRDPacketList = new ArrayList<RangRowDataPacket>();
	
	public NodeRowDataPacket(RouteResultsetNode node, int trimSize) {
		this.node = node;
		this.trimSize = trimSize;
	}
	
	public void newRang() {
		RangRowDataPacket rangPacket = new RangRowDataPacket();
		rangRDPacketList.add(rangPacket);
	}
	
	public long loadTotal() {
		return this.loadTrimTotal() + this.loadNotTrimTotal();
	}
	
	public long loadTrimTotal() {
		this.trimTotal = 0;
		for (RangRowDataPacket packet : trimRangRDPacketList) {
			if (packet.isTrim()) {
				this.trimTotal += packet.allSize();
			}
		}
		return this.trimTotal;
	}
	
	public long loadNotTrimTotal() {
		long total = 0;
		for (RangRowDataPacket packet : rangRDPacketList) {
			total += packet.allSize();
		}
		return total;
	}
	
	public void moveToTrim() {
		RangRowDataPacket head = this.loadHeadPacket();
        if (head != null && this.rangRDPacketList.remove(head)) {
            this.trimRangRDPacketList.add(head);
            if (head.allSize() == this.trimSize) {
                head.leftHeadTail();
            }
        }
	}
	
	public void moveHeadTail3ToTrim() {
		if (this.rangRDPacketList.size() >= 3) {
			int m = 0;
			while ((m = this.rangRDPacketList.size()) > 3) {
				RangRowDataPacket packet = this.rangRDPacketList.remove(0);
				if (packet.allSize() == this.trimSize) {
					packet.leftHeadTail();
				}
				addTrimWithCombine(packet);
			}
			
		}
	}
	
	private void addTrimWithCombine(RangRowDataPacket packet) {
		if (packet.allSize() == this.trimSize) {
			if (this.trimRangRDPacketList.isEmpty()) {
				this.trimRangRDPacketList.add(packet);
			} else {
				int last = this.trimRangRDPacketList.size() - 1;
				RangRowDataPacket lastPacket = 
					this.trimRangRDPacketList.get(last);
				if (lastPacket.isTrim()) {
					lastPacket.combine(packet);
				} else {
					//异常
				}
			}
		}
	}
	
	public void moveAllToTrim() {
		int m = 0;
		while ((m = this.rangRDPacketList.size()) > 0) {
			RangRowDataPacket packet = this.rangRDPacketList.remove(0);
			if (packet.getRowDataPacketList().size() == this.trimSize) {
				packet.leftHeadTail();
			}
			addTrimWithCombine(packet);
		}
	}
	
	public void addPacket(RowDataPacket packet) {
		RangRowDataPacket rangPacket = rangRDPacketList.get(rangRDPacketList.size() - 1);
		rangPacket.appendPacket(packet);
	}
	
	public RouteResultsetNode getNode() {
		return node;
	}
	
	public List<RowDataPacket> loadData() {
		List<RowDataPacket> result = new ArrayList<RowDataPacket>();
		for (RangRowDataPacket packet : rangRDPacketList) {
			result.addAll(packet.getRowDataPacketList());
		}
		for (RangRowDataPacket packet : trimRangRDPacketList) {
			if (!packet.isTrim()) {
				result.addAll(packet.getRowDataPacketList());
			}
		}
		return result;
	}
	
	public RangRowDataPacket loadHeadPacket() {
		if (rangRDPacketList != null && !rangRDPacketList.isEmpty()) {
			return rangRDPacketList.get(0);
		}
		return null;
	}
	
	public RangRowDataPacket loadTailPacket() {
		return this.loadTailPacket(1);
	}
	
	public RangRowDataPacket loadTailPacket(int tailIndex) {
		int size = rangRDPacketList.size() - tailIndex;
		if (size >= 0) {
			return rangRDPacketList.get(size);
		}
		return null;
	}
	
}