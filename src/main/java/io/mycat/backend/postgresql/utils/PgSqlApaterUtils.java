package io.mycat.backend.postgresql.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class PgSqlApaterUtils {
	/**
	 * 查询表结构
	 */
	private  static final  String SHOW_TABLE_STATUS_SQL_PREFIX ="SHOW TABLE STATUS LIKE";

	/*******
	 * 展示建表语句
	 */
	private  static  final  String SHOW_CREATE_TABLE_SQL_PREFIX  = "SHOW CREATE TABLE";

	/**
	 * 表机构信息 ,包含主键
	 */
	private  static  final  String  SHOW_COLUMNS_SQL_PREFIX ="SHOW COLUMNS FROM";



	public static String apater(String sql){
		sql =  sql.replaceAll("`","\"");
		final String SQL = sql.toUpperCase().replaceAll("`","\"");
		String _mapperSql  = stream.get(SQL);
		if(_mapperSql!=null){
			return _mapperSql;
		}
		if (SQL.startsWith(SHOW_TABLE_STATUS_SQL_PREFIX)){
			return doApaterTableStatusSql(SQL);
		}

		if(SQL.startsWith(SHOW_CREATE_TABLE_SQL_PREFIX)){
			return doApaterCreateTabelSql(SQL);
		}

		if(SQL.startsWith(SHOW_COLUMNS_SQL_PREFIX)){
			return doApaterColumnsSql(SQL);
		}

		if(SQL.indexOf("LIMIT")!=-1 && SQL.indexOf("OFFSET") == -1){//非pgsql 分页语句
			return  doApaterPagingSql(SQL,sql);
		}

		return sql;
	}


	/*******
	 *  获取列信息SQL  语句
	 * @param sql
	 * @return
     */
	private static String doApaterColumnsSql(String sql) {
		return "SELECT '' as \"Field\" ,'' as \"Type\" ,''as \"Null\" ,'' as \"Key\" ,'' as \"Default\" , '' as \"Extra\"  from pg_namespace where 1=2";
	}


	private static String doApaterPagingSql(final String SQL, String sql) {
		int index = SQL.indexOf("LIMIT");
		String pagingPart = sql.substring(index);
		String selectPart = sql.substring(0, index);
		String[] pk = pagingPart.split("(\\s+)|(,)");
		List<String> slices = new ArrayList<String>();
		for (String token : pk) {
			if (token.trim().length() > 0) {
				slices.add(token);
			}
		}

		if (slices.size() == 3) {
			return selectPart
					+ String.format("%s  %s  offset %s", slices.get(0),
							slices.get(2), slices.get(1));
		}
		if (slices.size() == 2) {
			return selectPart
					+ String.format(" %s %s offset 0 ", slices.get(0),
							slices.get(1));
		}

		return sql;// 无法处理分页sql原样返回
	}

	private static String doApaterCreateTabelSql(String sql) {
		return "select '' as Table ,'' as \"Create Table\" from pg_namespace where 1=2";
	}


	/********
	 * 进行表结构语句适配
	 * @param sql
	 * @return
     */
	private static String doApaterTableStatusSql(String sql) {
		String tableName  =sql.substring(SHOW_TABLE_STATUS_SQL_PREFIX.length());
		 StringBuilder sb = new StringBuilder();
		sb.append("SELECT").append(" ");
		sb.append("	attname AS NAME,").append(" ");
		sb.append("	'InnoDB' AS Engine,").append(" ");
		sb.append("	10 AS VERSION,").append(" ");
		sb.append("	'Compact' AS Row_format,").append(" ");
		sb.append("	0 AS ROWS,").append(" ");
		sb.append("	10000 AS Avg_row_length,").append(" ");
		sb.append("	10000 AS Data_length,").append(" ");
		sb.append("	0 AS Max_data_length,").append(" ");
		sb.append("	0 AS Index_length,").append(" ");
		sb.append("	0 AS Data_free,").append(" ");
		sb.append("	NULL AS Auto_increment,").append(" ");
		sb.append("	NULL AS Create_time,").append(" ");
		sb.append("	NULL AS Update_time,").append(" ");
		sb.append("	NULL AS Check_time,").append(" ");
		sb.append("	'utf8_general_ci' AS COLLATION,").append(" ");
		sb.append("	NULL AS Checksum,").append(" ");
		sb.append("	'' AS Create_options,").append(" ");
		sb.append("	'' AS COMMENT").append(" ");
		sb.append("FROM").append(" ");
		sb.append("	pg_attribute").append(" ");
		sb.append("INNER JOIN pg_class ON pg_attribute.attrelid = pg_class.oid").append(" ");
		sb.append("INNER JOIN pg_type ON pg_attribute.atttypid = pg_type.oid").append(" ");
		sb.append("LEFT OUTER JOIN pg_attrdef ON pg_attrdef.adrelid = pg_class.oid").append(" ");
		sb.append("AND pg_attrdef.adnum = pg_attribute.attnum").append(" ");
		sb.append("LEFT OUTER JOIN pg_description ON pg_description.objoid = pg_class.oid").append(" ");
		sb.append("AND pg_description.objsubid = pg_attribute.attnum").append(" ");
		sb.append("WHERE").append(" ");
		sb.append("	pg_attribute.attnum > 0").append(" ");
		sb.append("AND attisdropped <> 't'").append(" ");
		sb.append("AND pg_class.relname =").append(tableName).append(" ");
		sb.append("ORDER BY").append(" ");
		sb.append("	pg_attribute.attnum").append(" ");
		return  sb.toString();
	}


	public static Map<String, String>  stream = new HashMap<>();
	
	
	static{
		stream.put("SELECT @@CHARACTER_SET_DATABASE, @@COLLATION_DATABASE".toUpperCase(), "SELECT 'utf8' as \"@@character_set_database\", 'utf8_general_ci' as \"@@collation_database\"");
		stream.put("SHOW STATUS", "SELECT 'Aborted_clients' as \"Variable\" , 0 as \"Value\" where 1=2 ");
		stream.put("SHOW FULL TABLES WHERE Table_type != 'VIEW'".toUpperCase(), "select tablename as \"Tables_In_\",'BASE TABLE' as \"Table_Type\" from pg_tables where schemaname ='public'");
		stream.put("SHOW ENGINES","SELECT DISTINCT 'InnoDB' as Engine ,\t'DEFAULT' as Support , \t'Supports transactions,row-level locking and foreign keys' as \"Comment\"\t,'YES' as \"Transactions\" ,\t'YES' as \"XA\",'YES' as \"Savepoints\" from  pg_tablespace\n");
	}
}
