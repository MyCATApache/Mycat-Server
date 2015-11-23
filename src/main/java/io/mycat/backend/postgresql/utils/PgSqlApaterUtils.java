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
	}
}
