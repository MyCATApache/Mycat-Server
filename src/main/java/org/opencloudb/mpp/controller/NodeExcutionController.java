package org.opencloudb.mpp.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.mpp.MutiDataMergeService;
import org.opencloudb.mpp.RangRowDataPacketSorter;
import org.opencloudb.mpp.model.NodeRowDataPacket;
import org.opencloudb.mpp.model.RangRowDataPacket;
import org.opencloudb.mysql.nio.handler.NodeWithLimitHandler;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;

public class NodeExcutionController {
	
	private static final Logger LOGGER = Logger
				.getLogger(NodeExcutionController.class);

	private boolean autocommit;
	private int nextCount = 0;
	private int currCount = 0;
	private Map<String, Boolean> nextExcuteMap = new HashMap<String, Boolean>();
	
	private Map<String, ResponseHandler> nodeRpHandler = new HashMap<String, ResponseHandler>();
	private final RouteResultset rrs;
	private Map<String, NodeRowDataPacket> result = null;
	private MutiDataMergeService mergeService = null;
	
	private NonBlockingSession session = null;
	private RangRowDataPacketSorter sorter = null;
	
	private long minStartIndex = 0;
	
	public NodeExcutionController(RouteResultset rrs, Map<String, NodeRowDataPacket> result, 
			MutiDataMergeService mergeService) {
		this.rrs = rrs;
		this.result = result;
		this.mergeService = mergeService;
		
		minStartIndex = this.rrs.getLimitStart() - this.rrs.getNodes().length * this.rrs.getLimitSize();
		
		this.initNextExcute();
	}
	
	public void setSorter(RangRowDataPacketSorter sorter) {
		this.sorter = sorter;
	}
	
	public void initNextExcute() {
		nextCount = 0;
		RouteResultsetNode[] nodeArr = this.rrs.getNodes();
		for (RouteResultsetNode node : nodeArr) {
			nextExcuteMap.put(node.getName(), true);
			nextCount++;
		}
	}
	
	private ResponseHandler getNodeRpHandler(String dataNode) {
		return nodeRpHandler.get(dataNode);
	}
	
	private Map<String, Integer> nodePageIndexMap = new HashMap<String, Integer>();
	
	public void executeLastNode(RouteResultsetNode node) {
		BackendConnection conn = nodeBackendMap.get(node.getName());
		this._execute(conn, node);
	}
	
	private void changeNodeSql(RouteResultsetNode node) {
		String sql = node.getStatement();
		int index = sql.indexOf("OFFSET ");
		int lIndex = sql.indexOf("LIMIT ");
		int pageNum = 0;
		if (!nodePageIndexMap.containsKey(node.getName())) {
			pageNum++;
			nodePageIndexMap.put(node.getName(), pageNum);
		} else {
			pageNum = nodePageIndexMap.get(node.getName());
		}
		
		long offset = (pageNum - 1) * this.mergeService.getPagePatchSize();
		
		if (index != -1) {
			if (lIndex != -1) {
				sql = sql.substring(0, lIndex + 6) + this.mergeService.getPagePatchSize() + " OFFSET " + offset;
				node.setStatement(sql);
			}
		}
	}
	
	public void nextExcutePage() {
		Set<String> dataNodeNameSet = nextExcuteMap.keySet();
		for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			if (nextExcuteMap.get(dataNodeName)) {
				Integer index = nodePageIndexMap.get(dataNodeName);
				if (index == null) {
					index = 1;
					nodePageIndexMap.put(dataNodeName, index);
				}
				index++;
				nodePageIndexMap.put(dataNodeName, index);
			}
		}
	}
	
	private Map<String, BackendConnection> nodeBackendMap = new HashMap<String, BackendConnection>();
	public void _execute(BackendConnection conn, RouteResultsetNode node) {
		changeNodeSql(node);
		nodeBackendMap.put(node.getName(), conn);
		conn.setResponseHandler(this.getNodeRpHandler(node.getName()));
		try {
			conn.execute(node, session.getSource(), autocommit);
		} catch (IOException e) {
			this.mergeService.connectionError(e, conn);
		}
	}
	
	public void releaseAllBackend() {
		Set<String> dataNodeNameSet = nodeBackendMap.keySet();
		for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			BackendConnection backendConn = nodeBackendMap.get(dataNodeName);
			// realse this connection if safe
			session.releaseConnectionIfSafe(backendConn, LOGGER.isDebugEnabled(),true);
		}
	}
	
	public void initHandler(NonBlockingSession session) {
		this.session = session;
		this.autocommit = session.getSource().isAutocommit();
		RouteResultsetNode[] nodeArr = this.rrs.getNodes();
		for (RouteResultsetNode node : nodeArr) {
			NodeWithLimitHandler handler = new NodeWithLimitHandler(node, session, this.mergeService);
			nodeRpHandler.put(node.getName(), handler);
		}
	}
	
	public boolean canStop() {
		Set<String> nDataNodeNameSet = nextExcuteMap.keySet();
		for (Iterator<String> iter = nDataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			if (nextExcuteMap.get(dataNodeName)) {
				return false;
			}
		}
		
		return true;
	}
	
	private void trimPacket() {
		long lastTrimTotal = 0;
		Set<String> dataNodeNameSet = result.keySet();
		for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			NodeRowDataPacket nodePacket  = result.get(dataNodeName);
			
			lastTrimTotal += nodePacket.loadTrimTotal();
		}
		
		if (lastTrimTotal > minStartIndex) {
			return;
		}
		
		if (this.sorter == null) {
			long nextTrimTotal = lastTrimTotal;
			Set<String> dataNodeNameSet1 = result.keySet();
			for (Iterator<String> iter = dataNodeNameSet1.iterator(); iter.hasNext();) {
				String dataNodeName = iter.next();
				NodeRowDataPacket nodePacket  = result.get(dataNodeName);
				nextTrimTotal += nodePacket.loadNotTrimTotal();
			}
			
			if (nextTrimTotal < this.rrs.getLimitStart()) {
				Set<String> dataNodeNameSet2 = result.keySet();
				for (Iterator<String> iter = dataNodeNameSet2.iterator(); iter.hasNext();) {
					String dataNodeName = iter.next();
					NodeRowDataPacket nodePacket  = result.get(dataNodeName);
					nodePacket.moveToTrim();
				}
			}
			return;
		}
		
		//把前部分数据合并
		List<String> elList = new ArrayList<String>();
		Set<String> dataNodeNameSet1 = nextExcuteMap.keySet();
		for (Iterator<String> iter = dataNodeNameSet1.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			NodeRowDataPacket nodePacket = result.get(dataNodeName);
			
			RangRowDataPacket rangRowDataPacket = nodePacket.loadTailPacket();
			if (rangRowDataPacket == null) {
				continue;
			}
			
			RangRowDataPacket rangRowDataPacket1 = nodePacket.loadTailPacket(2);
			if (rangRowDataPacket.allSize() < this.rrs.getLimitSize()) {
				elList.add(dataNodeName);
				if (rangRowDataPacket1 == null) {
					continue;
				}
			}
			if (rangRowDataPacket1 == null) {
				continue;
			}
			
			nodePacket.moveHeadTail3ToTrim();
		}
		
		boolean ascDesc = this.sorter.ascDesc(0);
		//把不足的数据节点移走
		for (String dn : elList) {
			NodeRowDataPacket nodePacket = result.get(dn);
			
			RangRowDataPacket rangRowDataPacket = nodePacket.loadTailPacket();
			if (rangRowDataPacket == null || rangRowDataPacket.getRowDataPacketList().isEmpty()) {
				rangRowDataPacket = nodePacket.loadTailPacket(2);
				
				if (rangRowDataPacket == null || rangRowDataPacket.getRowDataPacketList().isEmpty()) {
					continue;
				}
			}
			
			RowDataPacket tailPacket = rangRowDataPacket.getTail();
			if (tailPacket == null) {
				tailPacket = rangRowDataPacket.getHead();
			}
			
			Set<String> dataNodeNameSet2 = nextExcuteMap.keySet();
			for (Iterator<String> iter = dataNodeNameSet2.iterator(); iter.hasNext();) {
				String dataNodeName = iter.next();
				if (!dataNodeName.equals(dn)) {
					NodeRowDataPacket nodePacket2 = result.get(dataNodeName);
					
					RangRowDataPacket rangRowDataPacket2 = nodePacket2.loadHeadPacket();
					if (rangRowDataPacket2 == null) {
						continue;
					}
					
					RowDataPacket headPacket = rangRowDataPacket2.getHead();
					if (headPacket == null) {
						continue;
					}
					int headResutl = this.sorter.compareRowData(tailPacket, headPacket, 0);
					if (ascDesc) {
						if (headResutl < 0) {
							nodePacket.moveAllToTrim();
						}
					} else {
						if (headResutl > 0) {
							nodePacket.moveAllToTrim();
						}
					}
				}
			}
		}
	}
	
	private void nextExcuteFalse() {
		Set<String> eDataNodeNameSet = nextExcuteMap.keySet();
		for (Iterator<String> iter = eDataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			nextExcuteMap.put(dataNodeName, false);
		}
	}
	
	private void nextExcuteTrue() {
		Set<String> eDataNodeNameSet = nextExcuteMap.keySet();
		for (Iterator<String> iter = eDataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			nextExcuteMap.put(dataNodeName, true);
		}
	}
	
	private void nextExcuteTrueWithClear(List<String> dataNodeList) {
		this.nextExcuteFalse();
		for (String dn : dataNodeList) {
			nextExcuteMap.put(dn, true);
		}
	}
	
	private void nextExcuteTrue(List<String> dataNodeList) {
		for (String dn : dataNodeList) {
			nextExcuteMap.put(dn, true);
		}
	}
	
	private boolean isEnd = false;
	private void controllNext() {
		this.trimPacket();
		long total = 0;
		Set<String> dataNodeNameSet = result.keySet();
		Map<String, Boolean> fullMap = new HashMap<String, Boolean>();
		for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			NodeRowDataPacket nodePacket = result.get(dataNodeName);
			total += nodePacket.loadTotal();
			
			RangRowDataPacket rPacket = nodePacket.loadTailPacket();
			if (rPacket != null && rPacket.allSize() == this.mergeService.getPagePatchSize()) {
				fullMap.put(dataNodeName, true);
			}
		}
		
		//没有数据加载的后面不再加载
		Set<String> eDataNodeNameSet = nextExcuteMap.keySet();
		for (Iterator<String> iter = eDataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			Long lastCount = nodeExcuteAddCountMap.get(dataNodeName);
			if (lastCount == null || lastCount.compareTo((long) this.mergeService.getPagePatchSize()) < 0) {
				nextExcuteMap.put(dataNodeName, false);
			}
		}
		
		if (this.sorter == null) {
			return;
		}
		
		Set<String> fullDataNodeNameSet = fullMap.keySet();
		for (Iterator<String> iter = fullDataNodeNameSet.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			
			if (fullMap.get(dataNodeName) && !nextExcuteMap.get(dataNodeName)) {
				nextExcuteMap.put(dataNodeName, true);
			}
		}
		
		//识别可继续加载node
		List<String> nextDataNameList = new ArrayList<String>();
		Set<String> eDataNodeNameSet1 = nextExcuteMap.keySet();
		for (Iterator<String> iter = eDataNodeNameSet1.iterator(); iter.hasNext();) {
			String dataNodeName = iter.next();
			if (nextExcuteMap.get(dataNodeName)) {
				nextDataNameList.add(dataNodeName);
			}
		}
		
		//不需要再加载就返回
		if (nextDataNameList.size() == 1) {
			if (total - this.rrs.getLimitStart() >= this.rrs.getLimitSize()) {
				if (isEnd == false) {
					isEnd = true;
				} else {
					nextExcuteMap.put(nextDataNameList.get(0), false);
					return;
				}
			}
		}
		
		
		//得到头部处理最后
		boolean ascDesc = this.sorter.ascDesc(0);
		RowDataPacket lastHeadTail = null;
		String lastRandHeadTailNode = null;
		for (String dataName : nextDataNameList) {
			NodeRowDataPacket nodePacket = result.get(dataName);
			RangRowDataPacket rangPacket = nodePacket.loadTailPacket();
			if (rangPacket == null) {
				continue;
			}
			RowDataPacket headPacket = rangPacket.getHead();
			if (lastHeadTail == null) {
				lastHeadTail = headPacket;
				lastRandHeadTailNode = dataName;
				continue;
			}
			
			int headResutl = this.sorter.compareRowData(lastHeadTail, headPacket, 0);
			if (ascDesc) {
				if (headResutl < 0) {
					lastHeadTail = headPacket;
					lastRandHeadTailNode = dataName;
				}
			} else {
				if (headResutl > 0) {
					lastHeadTail = headPacket;
					lastRandHeadTailNode = dataName;
				}
			}
		}
		
		//得到尾部在上面得到的头部前面节点
		List<String> newNextDataList = new ArrayList<String>();
		for (String dataName : nextDataNameList) {
			NodeRowDataPacket nodePacket = result.get(dataName);
			RangRowDataPacket rangPacket = nodePacket.loadTailPacket();
			if (rangPacket == null || rangPacket.getRowDataPacketList().isEmpty()) {
				continue;
			}
			RowDataPacket tailPacket = rangPacket.getTail();
			if (tailPacket == null) {
				tailPacket = rangPacket.getHead();
			}
			int headResutl = this.sorter.compareRowData(lastHeadTail, tailPacket, 0);
			if (ascDesc) {
				if (headResutl > 0) {
					newNextDataList.add(dataName);
				}
			} else {
				if (headResutl < 0) {
					newNextDataList.add(dataName);
				}
			}
		}
		
		//出现断层数据就先加载
		if (!newNextDataList.isEmpty()) {
			this.nextExcuteTrueWithClear(newNextDataList);
			return;
		}
		
		if (total < this.rrs.getLimitStart()) {
			List<String> newHeadNextDataList = new ArrayList<String>();
			for (String dataName : nextDataNameList) {
				if (!dataName.equals(lastRandHeadTailNode)) {
					newHeadNextDataList.add(dataName);
				}
			}
			if (!newHeadNextDataList.isEmpty()) {
				this.nextExcuteTrueWithClear(newHeadNextDataList);
				return;
			}
		}
	}
	
	private Map<String, Long> nodeExcuteAddCountMap = new HashMap<String, Long>();
	public void newRecord(String dataNode) {
		Long count = nodeExcuteAddCountMap.get(dataNode);
		if (count == null) {
			count = 0L;
		}
		count++;
		nodeExcuteAddCountMap.put(dataNode, count);
	}
	
	public void dataOk(String dataNode, byte[] eof, BackendConnection conn) {
		if (nextExcuteMap.get(dataNode)) {
			currCount++;
		}
		if (currCount == nextCount) {
			controllNext();
			
			if (this.canStop()) {
				this.mergeService.rowEofResponse(eof, null);
			} else {
				nextCount = 0;
				Set<String> countDataNodeNameSet = nextExcuteMap.keySet();
				for (Iterator<String> iter = countDataNodeNameSet.iterator(); iter.hasNext();) {
					String dataNodeName = iter.next();
					if (nextExcuteMap.get(dataNodeName)) {
						nextCount++;
					}
				}
				
				currCount = 0;
				nodeExcuteAddCountMap.clear();
				
				Set<String> dataNodeNameSet = nextExcuteMap.keySet();
				this.nextExcutePage();
				for (Iterator<String> iter = dataNodeNameSet.iterator(); iter.hasNext();) {
					String dataNodeName = iter.next();
					if (nextExcuteMap.get(dataNodeName)) {
						this.mergeService.onNewRangRecord(dataNodeName);
						this.executeLastNode(result.get(dataNodeName).getNode());
					}
				}
			}
		}
	}
}
