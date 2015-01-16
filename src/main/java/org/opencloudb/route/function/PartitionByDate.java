package org.opencloudb.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.rule.RuleAlgorithm;
import org.opencloudb.exception.PartitionException;

/**
 * 例子 按日期列分区  格式 between操作解析的范例
 * 
 * @author lxy
 * 
 */
public class PartitionByDate extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	private static final Logger LOGGER = Logger.getLogger(PartitionByDate.class);
	
	private static final String GROUP_SPLITOR=";";
	private static final String NODES_SPLITOR=",";
	private static final long ONE_DAY_MILLIS = 86400000;

	/**
	 * date to shard begin with sBetinDate
	 */
	private String sBeginDate;
	private String dateFormat;

	private long beginDate;
	private long partionTime;
	/**
	 * each group has one or more nodes, the real value of column may be so large to be out of group length, so it will be calculate mod if groupMod is true, or ArrayIndexOutOfBoundException
	 */
	private boolean groupMod;
	private int[][] nodes;
	
	private boolean grouped=false;
	

	@Override
	public void init() {
		try {
			beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Integer calculate(String columnValue) {
		try {
			long targetTime = new SimpleDateFormat(dateFormat).parse(columnValue).getTime();
			int targetPartition = (int) ((targetTime - beginDate) / partionTime);
			if(!grouped){
				return targetPartition;
			}
			int[] groupNodes=null;
			if(!groupMod){
				if(targetPartition>=nodes.length){
					throw new ArrayIndexOutOfBoundsException("node count: "+nodes.length+", targetPartition: "+targetPartition);
				}
				groupNodes=nodes[targetPartition];
			}else{
				groupNodes=nodes[targetPartition%nodes.length];
			}
			return groupNodes[(int)(targetTime%groupNodes.length)];
		} catch (ParseException|ArrayIndexOutOfBoundsException e) {
			LOGGER.error("date partition rule wrong", e);
		}
		return null;
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setsPartionDay(String sPartionDay) {
		partionTime = Integer.parseInt(sPartionDay) * ONE_DAY_MILLIS;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	/**
	 * nodes to shard
	 * format: 0,1,2,3;4,5;6,7,8
	 * @param nodesText
	 */
	public void setNodesText(String nodesText){
		StringTokenizer groups=new StringTokenizer(nodesText.trim(),GROUP_SPLITOR);
		nodes=new int[groups.countTokens()][];
		for(int i=0,il=nodes.length;i<il;i++){
			StringTokenizer groupNodes=new StringTokenizer(groups.nextToken().trim(),NODES_SPLITOR);
			int[] nodesArray=nodes[i]=new int[groupNodes.countTokens()];
			for(int j=0,jl=nodesArray.length;j<jl;j++){
				nodesArray[j]=Integer.parseInt(groupNodes.nextToken().trim());
			}
		}
		grouped=true;
	}
	public void setGroupMode(String groupMod){
		this.groupMod=Boolean.parseBoolean(groupMod);
	}
}
