package io.mycat.backend.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;

import io.mycat.backend.mysql.DataType;

/**
 * 查询中间结果处理器
 * @author huangyiming
 *
 * @param <T>
 */
public class MiddlerQueryResultHandler<T> implements MiddlerResultHandler<T> {

	List<T> reusult = new ArrayList<T>();
	DataType dataType;
	
	private SecondHandler secondHandler;
	
	public MiddlerQueryResultHandler(DataType dataType,SecondHandler secondHandler) {
		this.dataType = dataType;
		this.secondHandler = secondHandler;
 	}
	//确保只有一个构造函数入口
	private MiddlerQueryResultHandler(){
		
	}
	
	@Override
	public List<T> getResult() {
 		return reusult;
	}
	@Override
	public void add(T t ) {
 		reusult.add(t);
 	}

	 
	public static void main(String[] args) {
		System.out.println(String.class);
		
	}
	@Override
	public String getDataType() {
 		return dataType.name();
	}
	
	@Override
	public void secondEexcute() {
		List<T> list =   getResult();
		StringBuffer sb = new StringBuffer();
		 for(T t:list){
			 sb.append(t).append(",");
		 }
		  
		secondHandler.doExecute(sb.substring(0, sb.length()-1));
		
	}
	
	 
	 
}
