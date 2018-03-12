package io.mycat.route.function;

/**
 * Latest one month data partions ,only reserve data of latest 31 days and one
 * day is partioned into N slide (splitOneDay), so total datanode is M*N table's
 * partion column must be int type and it's value format should be yyyyMMddHH
 * fomat for example colmn=2014050115 means: 15 clock of april 5 ,2014
 * 
 * @author wuzhih
 * 
 */
public class LatestMonthPartion extends AbstractPartitionAlgorithm {
	private int splitOneDay = 24;
	private int hourSpan;
	private String[] dataNodes;

	public String[] getDataNodes() {
		return dataNodes;
	}

	/**
	 * @param dataNodeExpression
	 */
	public void setSplitOneDay(int split) {
		splitOneDay = split;
		hourSpan = 24 / splitOneDay;
		if (hourSpan * 24 < 24) {
			throw new java.lang.IllegalArgumentException(
					"invalid splitOnDay param:"
							+ splitOneDay
							+ " should be an even number and less or equals than 24");
		}
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			int valueLen = columnValue.length();
			int day = Integer.parseInt(columnValue.substring(valueLen - 4,
					valueLen - 2));
			int hour = Integer.parseInt(columnValue.substring(valueLen - 2));
			int dnIndex = (day - 1) * splitOneDay + hour / hourSpan;
			return dnIndex;
		}catch (NumberFormatException e){
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	public Integer[] calculateRange(String beginValue, String endValue)  {
		return calculateSequenceRange(this,beginValue, endValue);
	}

}
