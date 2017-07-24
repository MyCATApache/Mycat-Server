package io.mycat.backend.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;

import io.mycat.backend.mysql.DataType;

/**
 * 查询中间结果处理器
 * @author huangyiming
 *
 * @param <T>
 */
public class MiddlerQueryResultHandler<T> implements MiddlerResultHandler<T> {

	List<SQLCharExpr> reusult = new ArrayList<>();
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
	public List<SQLCharExpr> getResult() {
 		return reusult;
	}
	@Override
	public void add(T t ) {
 		reusult.add(new SQLCharExpr(t==null?null:t.toString()));
 	}	 
	 
	@Override
	public String getDataType() {
 		return dataType.name();
	}
	
	@Override
	public void secondEexcute() {
		secondHandler.doExecute(getResult());
	}
}
