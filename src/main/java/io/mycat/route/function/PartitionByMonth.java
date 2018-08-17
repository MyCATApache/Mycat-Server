package io.mycat.route.function;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.util.StringUtil;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
	/** 默认格式 */
	private String dateFormat = "yyyy-MM-dd";
	/** 场景 */
	private int scene = -1;
	private String sEndDate;
	private Calendar beginDate;
	private Calendar endDate;
	private int nPartition;

	private ThreadLocal<SimpleDateFormat> formatter;

	@Override
	public void init() {
		try {
			if (StringUtil.isEmpty(sBeginDate) && StringUtil.isEmpty(sEndDate)) {
				nPartition = 12;
				scene = 1;
				initFormatter();
				beginDate = Calendar.getInstance();
				beginDate.set(Calendar.MONTH, 0);
				endDate = Calendar.getInstance();
				endDate.set(Calendar.MONTH, 11);
				return;
			}
			beginDate = Calendar.getInstance();
			beginDate.setTime(new SimpleDateFormat(dateFormat)
									  .parse(sBeginDate));
			initFormatter();
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

	private void initFormatter() {
		formatter = new ThreadLocal<SimpleDateFormat>() {
            @Override
            protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(dateFormat);
            }
        };
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
	public Integer calculate(String columnValue)  {
		try {
			if (scene == 1) {
				Calendar curTime = Calendar.getInstance();
				curTime.setTime(formatter.get().parse(columnValue));
				return curTime.get(Calendar.MONTH);
			}
			int targetPartition;
			Calendar curTime = Calendar.getInstance();
			curTime.setTime(formatter.get().parse(columnValue));
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
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		try {
			return doCalculateRange(beginValue, endValue,beginDate);
		} catch (ParseException e) {
			LOGGER.error("error",e);
			return new Integer[0];
		}
	}

	private Integer[] doCalculateRange(String beginValue, String endValue,Calendar beginDate) throws ParseException {
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
		// 当在场景1： "2015-01-01", "2014-04-03" 范围出现的时候
		// 是应该返回null 还是返回 [] ?
		return (list.toArray(new Integer[size]));
	}

	@Override
	public int getPartitionNum() {
		int nPartition = this.nPartition;
		return nPartition;
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
