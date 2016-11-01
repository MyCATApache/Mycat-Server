package org.opencloudb.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.pattern.IntegerPatternConverter;
import org.opencloudb.config.model.rule.RuleAlgorithm;

/**
 * 例子 按月份列分区 ，每个自然月一个分片，格式 between操作解析的范例
 * 
 * @author wzh
 * 
 */
public class PartitionByMonth extends AbstractPartitionAlgorithm implements
		RuleAlgorithm {
	private static final Logger LOGGER = Logger.getLogger(PartitionByDate.class);
	private String sBeginDate;
	private String dateFormat;
	private String sEndDate;
	private Calendar beginDate;
	private Calendar endDate;
	private int nPartition;

	@Override
	public void init() {
		try {
			beginDate = Calendar.getInstance();
			beginDate.setTime(new SimpleDateFormat(dateFormat)
					.parse(sBeginDate));

			if(sEndDate!=null&&!sEndDate.equals("")) {
				endDate = Calendar.getInstance();
				endDate.setTime(new SimpleDateFormat(dateFormat).parse(sEndDate));
				nPartition = ((endDate.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR)) * 12
								+ endDate.get(Calendar.MONTH) - beginDate.get(Calendar.MONTH)) + 1;

				if (nPartition <= 0) {
					throw new java.lang.IllegalArgumentException("Incorrect time range for month partitioning!");
				}
			} else {
				nPartition = -1;
			}
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	/**
	 * For circulatory partition, calculated value of target partition needs to be
	 * rotated to fit the partition range
	 */
	private int reCalculatePartition(int targetPartition) {
		/**
		 * If target date is previous of start time of partition setting, shift
		 * the delta range between target and start date to be positive value
		 */
		if (targetPartition < 0) {
			targetPartition = nPartition - (-targetPartition) % nPartition;
		}

		if (targetPartition >= nPartition) {
			targetPartition =  targetPartition % nPartition;
		}

		return targetPartition;
	}

	@Override
	public Integer calculate(String columnValue) {
		try {
			int targetPartition;
			Calendar curTime = Calendar.getInstance();
			curTime.setTime(new SimpleDateFormat(dateFormat).parse(columnValue));
			targetPartition = ((curTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
					* 12 + curTime.get(Calendar.MONTH)
					- beginDate.get(Calendar.MONTH));

			/**
			 * For circulatory partition, calculated value of target partition needs to be
			 * rotated to fit the partition range
 			 */
			if (nPartition > 0) {
				targetPartition = reCalculatePartition(targetPartition);
			}
			return targetPartition;

		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		try {
			int startPartition, endPartition;
			Calendar partitionTime = Calendar.getInstance();
			SimpleDateFormat format = new SimpleDateFormat(dateFormat);
			partitionTime.setTime(format.parse(beginValue));
			startPartition = ((partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
					* 12 + partitionTime.get(Calendar.MONTH)
					- beginDate.get(Calendar.MONTH));
			partitionTime.setTime(format.parse(endValue));
			endPartition = ((partitionTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
					* 12 + partitionTime.get(Calendar.MONTH)
					- beginDate.get(Calendar.MONTH));

			List<Integer> list = new ArrayList<>();

			while (startPartition <= endPartition) {
				Integer nodeValue = reCalculatePartition(startPartition);
				if (Collections.frequency(list, nodeValue) < 1)
					list.add(nodeValue);
				startPartition++;
			}
			int size = list.size();
			return (list.toArray(new Integer[size]));
		} catch (ParseException e) {
			LOGGER.error("error",e);
			return new Integer[0];
		}
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public void setsEndDate(String sEndDate) {
		this.sEndDate = sEndDate;
	}

}
