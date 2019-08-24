package io.mycat.backend.mysql.nio.handler;

import java.util.List;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

/**
 * 中间结果处理器
 * @author huangyiming
 *
 * @param <T>
 */
public interface MiddlerResultHandler<T> {

 	
	public List<SQLCharExpr> getResult();
	
	public void  add(T t );
	
	public String getDataType();
	
	public void secondEexcute();
	
	
	  
	
	
	
 }
