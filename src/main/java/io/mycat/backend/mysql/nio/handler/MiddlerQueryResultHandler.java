package io.mycat.backend.mysql.nio.handler;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
	Class<T> clazz;
	private SecondHandler secondHandler;
	
	public MiddlerQueryResultHandler(SecondHandler secondHandler) {
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

	 
	 
	@Override
	public String getDataType() {
 		return dataType.name();
	}
	
	@Override
	public void secondEexcute() {
		List<T> list =   getResult();
		StringBuffer sb = new StringBuffer();
		
		 for(T t:list){
			 sb.append('\'');
			 sb.append(t).append('\'').append(",");
		 }
		 
		 String param = "";
		  if(sb.equals("")|| sb.length()==0){
			  param="";
		  }else{
			  param = sb.substring(0, sb.length()-1);
		  }
 		  
		secondHandler.doExecute(param);
		
	}
	
	 
	 
}
