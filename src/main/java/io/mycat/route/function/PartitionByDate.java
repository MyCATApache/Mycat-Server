package io.mycat.route.function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 例子 按日期列分区  格式 between操作解析的范例
 *
 * @author lxy
 *
 */
public class PartitionByDate extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDate.class);

	private String sBeginDate;
	private String sPartionDay;
	private String dateFormat;

	private long beginDate;
	private long partionTime;

	private static final long oneDay = 86400000;

	@Override
	public void init() {
		try {
			beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate)
					.getTime();
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
		partionTime = Integer.parseInt(sPartionDay) * oneDay;
	}

	@Override
	public Integer calculate(String columnValue) {
		try {
			long targetTime = new SimpleDateFormat(dateFormat).parse(
					columnValue).getTime();
			int targetPartition = (int) ((targetTime - beginDate) / partionTime);
			return targetPartition;

		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);

		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setsPartionDay(String sPartionDay) {
		this.sPartionDay = sPartionDay;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

}
