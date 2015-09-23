package io.mycat.route.function;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 例子 按月份列分区 ，每个自然月一个分片，格式 between操作解析的范例
 * 
 * @author wzh
 * 
 */
public class PartitionByMonth extends AbstractPartitionAlgorithm implements
		RuleAlgorithm {
	private String sBeginDate;
	private String dateFormat;
	private Calendar beginDate;

	@Override
	public void init() {
		try {
			beginDate = Calendar.getInstance();
			beginDate.setTime(new SimpleDateFormat(dateFormat)
					.parse(sBeginDate));
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer calculate(String columnValue) {
		try {
			Calendar curTime = Calendar.getInstance();
			curTime.setTime(new SimpleDateFormat(dateFormat).parse(columnValue));
			return (curTime.get(Calendar.YEAR) - beginDate.get(Calendar.YEAR))
					* 12 + curTime.get(Calendar.MONTH)
					- beginDate.get(Calendar.MONTH);

		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue) {
		return AbstractPartitionAlgorithm.calculateSequenceRange(this,
				beginValue, endValue);
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

}
