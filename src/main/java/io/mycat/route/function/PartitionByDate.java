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
	private String sEndDate;
	private String sPartionDay;
	private String dateFormat;

	private long beginDate;
	private long partionTime;
	private long endDate;
	private int nCount;

	
	private static final long oneDay = 86400000;

	@Override
	public void init() {
		try {
			partionTime = Integer.parseInt(sPartionDay) * oneDay;
			
			beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

			if(sEndDate!=null&&!sEndDate.equals("")){
			    endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
			    nCount = (int) ((endDate - beginDate) / partionTime) + 1;
			}
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer calculate(String columnValue) {
		try {
			long targetTime = new SimpleDateFormat(dateFormat).parse(columnValue).getTime();
			int targetPartition = (int) ((targetTime - beginDate) / partionTime);

			if(targetTime>endDate && nCount!=0){
				targetPartition = targetPartition%nCount;
			}
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
	public String getsEndDate() {
		return this.sEndDate;
	}
	public void setsEndDate(String sEndDate) {
		this.sEndDate = sEndDate;
	}

}
