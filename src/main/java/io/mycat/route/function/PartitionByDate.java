package io.mycat.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.config.model.rule.RuleAlgorithm;

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

	private ThreadLocal<SimpleDateFormat> formatter;
	
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
			formatter = new ThreadLocal<SimpleDateFormat>() {
				@Override
				protected SimpleDateFormat initialValue() {
					return new SimpleDateFormat(dateFormat);
				}
			};
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			long targetTime = formatter.get().parse(columnValue).getTime();
			int targetPartition = (int) ((targetTime - beginDate) / partionTime);

			if(targetTime>endDate && nCount!=0){
				targetPartition = targetPartition%nCount;
			}
			return targetPartition;

		} catch (ParseException e) {
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue)  {
		SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
		try {
			Date beginDate = format.parse(beginValue);
			Date endDate = format.parse(endValue);
			Calendar cal = Calendar.getInstance();
			List<Integer> list = new ArrayList<Integer>();
			while(beginDate.getTime() <= endDate.getTime()){
				Integer nodeValue = this.calculate(format.format(beginDate));
				if(Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
				cal.setTime(beginDate);
				cal.add(Calendar.DATE, 1);
				beginDate = cal.getTime();
			}

			Integer[] nodeArray = new Integer[list.size()];
			for (int i=0;i<list.size();i++) {
				nodeArray[i] = list.get(i);
			}

			return nodeArray;
		} catch (ParseException e) {
			LOGGER.error("error",e);
			return new Integer[0];
		}
	}
	
	@Override
	public int getPartitionNum() {
		int count = this.nCount;
		return count > 0 ? count : -1;
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
