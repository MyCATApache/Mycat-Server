package io.mycat.backend.mysql.nio.handler;

import java.util.List;

import io.mycat.backend.mysql.DataType;

/**
 * 中间结果处理器
 * @author huangyiming
 *
 * @param <T>
 */
public interface MiddlerResultHandler<T> {

 	
	public List<T> getResult();
	
	public void  add(T t );
	
	public String getDataType();
	
	public void secondEexcute();
	
	
	  
	
	
	
 }
