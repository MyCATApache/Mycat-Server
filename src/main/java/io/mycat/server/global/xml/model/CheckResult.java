package io.mycat.server.global.xml.model;

import java.util.List;

public class CheckResult {

	private List<DataNode> dataNodes;

	private static CheckResult instance = new CheckResult();

	private CheckResult() {

	}

	public static CheckResult getInstance() {
		return instance;
	}

	public List<DataNode> getDataNodes() {
		return dataNodes;
	}

	public void setDataNodes(List<DataNode> dataNodes) {
		this.dataNodes = dataNodes;
	}

	public DataNode getDateNodeByName(String dataNodeName) {
		for(DataNode dataNode:dataNodes) {
			if(dataNodeName.equals(dataNode.getName())){
				return dataNode;
			}
		}
		return null;
	}

}
