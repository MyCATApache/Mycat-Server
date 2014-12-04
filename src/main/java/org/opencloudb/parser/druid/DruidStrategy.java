package org.opencloudb.parser.druid;

import java.sql.SQLNonTransientException;

import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.route.RouteResultset;

import com.alibaba.druid.sql.ast.SQLStatement;

/**
 * Druid解析器解析策略接口，可以扩展数据库类型
 * @author wang.dw
 *
 */
public interface DruidStrategy {
	/**
	 * 元数据SQL，如select 语句
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseSelectSQL(SchemaConfig schema,RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
	
	/**
	 * 元数据SQL，如insert into tableName(column1,column2，...) values (value1,value2,...);
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseInsertSQL(SchemaConfig schema,RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;

	/**
	 * 元数据SQL，如delete from tableName where id = 1
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseDeleteSQL(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
	
	/**
	 * 元数据SQL，如update tableName set name='zhangsan';
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseUpdateSQL(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
	
	/**
	 * 元数据SQL，其他sql类型默认使用该方法解析
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseDefault(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
	
	/**
	 * 元数据SQL，create table
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseCreateTable(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
	
	
	/**
	 * 元数据SQL，create table
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @return
	 */
	public abstract RouteResultset analyseAlterTable(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException;
}
