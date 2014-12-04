package org.opencloudb.route;

import org.junit.Before;
import org.junit.Test;

public class RouteServiceTest {

	@Before
	public void setUp() throws Exception {
	}

	/**
	 * show语句
	 */
	@Test
	public void testShow() {
		
	}
	
	/**
	 * select @@
	 */
	@Test
	public void testDoubleAtSgin() {
		
	}
	

	/**
	 * select @@
	 */
	@Test
	public void testDescribe() {
		
	}
	
	/**
	 * select @@
	 */
	@Test
	public void testSelect() {
		
	}
	
	/**
	 * insert
	 */
	@Test
	public void testInsert() {
		
	}
	
	/**
	 * delete
	 */
	@Test
	public void testDelete() {
		
	}
	
	/**
	 * DDL
	 */
	@Test
	public void testDDL() {
		testCreateTable();
		testAlterTable();
		testDropTable();
		testTruncateTable();
	}
	
	/**
	 * create table
	 */
	@Test
	public void testCreateTable() {
//		1、create table tablename(id int,name varchar(10));
//		2、create table tablename select * from othertable;
	}
	
	/**
	 * alter table
	 */
	@Test
	public void testAlterTable() {
	
	}
	
	/**
	 * drop table
	 */
	@Test
	public void testDropTable() {

	}
	
	/**
	 * truncate table
	 */
	@Test
	public void testTruncateTable() {
		//truncate table tableName
	}
	
	/**
	 * truncate table
	 */
	@Test
	public void testCreateIndex() {
		//普通索引
		
		//唯一索引
//		UNIQUE 
//		支持的语法：
//		alter table coding_rule add unique (prefix);
//		不支持的语法（带索引名称的不支持）：
//		alter table coding_rule add unique ux_indexname (prefix);
//		create unique index ux_indexname on coding_rule (prefix) ;
	}
	
	/**
	 * drop index
	 */
	@Test
	public void testDropIndex() {
		
	}
	
	/**
	 * alter table 改字段名等
	 */
	@Test
	public void testMorify() {
		
	}
	
	/**
	 * alter table change....
	 */
	@Test
	public void testChange() {
		
	}
	
	
	
	
	/**
	 * 修改表名	alter table customer1 rename to coding_rule; 	
修改字段名	alter table t1 change c1 c1 varchar（44）;	　	
修改数据库的字符集	alter database maildb default character set utf8;	　
添加主键	alter table tb add primary key(id);	
删除主键（自增主键）	alter table coding_rule modify id  int COMMENT '主键';
alter table coding_rule drop primary key	
删除主键（非自增）	alter table sale_out_storage drop primary key;	自己修改	支持
	 */
	
	
	//一些特殊场景的测试用例：如global table的insert 要操作所有分片，select只需要随机返回一个分片
	//insert语句不能有批量insert.....
}
