package io.mycat.backend.postgresql.utils;

import java.util.HashMap;
import java.util.Map;



public class PgSqlApaterUtils {
	public static String apater(String sql){
		if(stream.get(sql.toUpperCase())!=null){
			return stream.get(sql.toUpperCase());
		}
		return sql;
	}
	
	public static Map<String, String>  stream = new HashMap<>();
	
	
	static{
		stream.put("SELECT @@CHARACTER_SET_DATABASE, @@COLLATION_DATABASE".toUpperCase(), "SELECT 'utf8' as \"@@character_set_database\", 'utf8_general_ci' as \"@@collation_database\"");
		stream.put("SHOW STATUS", "SELECT 'Aborted_clients' as \"Variable\" , 0 as \"Value\" where 1=2 ");
		stream.put("SHOW FULL TABLES WHERE Table_type != 'VIEW'".toUpperCase(), "select tablename as \"Tables_In_\",'BASE TABLE' as \"Table_Type\" from pg_tables where schemaname ='public'");
	//	stream.put("SHOW TABLE STATUS LIKE 'company'".toUpperCase(), "select 1 where 1=2");
	}
}
