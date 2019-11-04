package io.mycat.server.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.parser.ServerParse;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by magicdoom on 2016/1/26.
 */
public class SchemaUtil {
    public static SchemaInfo parseSchema(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        return parseTables(parser.parseStatement(), new MycatSchemaStatVisitor());
    }

    public static String detectDefaultDb(String sql, int type) {
        String db = null;
        Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig()
                .getSchemas();
        if (ServerParse.SELECT == type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if ((schemaInfo == null || schemaInfo.table == null) && !schemaConfigMap.isEmpty()) {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }

            if (schemaInfo != null && schemaInfo.schema != null) {

                if (schemaConfigMap.containsKey(schemaInfo.schema)) {
                    db = schemaInfo.schema;

                    /**
                     * 对 MySQL 自带的元数据库 information_schema 进行返回
                     */
                } else if ("information_schema".equalsIgnoreCase(schemaInfo.schema)) {
                    db = "information_schema";
                }
            }
        } else if (ServerParse.INSERT == type || ServerParse.UPDATE == type || ServerParse.DELETE == type || ServerParse.DDL == type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if (schemaInfo != null && schemaInfo.schema != null && schemaConfigMap.containsKey(schemaInfo.schema)) {
                db = schemaInfo.schema;
            }
        } else if(ServerParse.COMMAND == type) {
            int index = sql.indexOf(ServerParse.COM_FIELD_LIST_FLAG);
            String table = sql.substring(index + 17);
            db = getSchema(schemaConfigMap, table);
        } else if ((ServerParse.SHOW == type || ServerParse.USE == type || ServerParse.EXPLAIN == type || ServerParse.SET == type
                || ServerParse.HELP == type || ServerParse.DESCRIBE == type)
                && !schemaConfigMap.isEmpty()) {
            try {
                //当前存在多个schema的时候使用原来的逻辑会出现bug，改为根据mycat schema配置中的对应关系获取
                SQLStatementParser parser = new MySqlStatementParser(sql);
                MySqlShowColumnsStatement stmt = (MySqlShowColumnsStatement)parser.parseStatement();
                db = getSchema(schemaConfigMap, stmt.getTable().getSimpleName());
            } catch (Exception e) {
                //e.printStackTrace();
                //兼容mysql gui  不填默认database
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }
        }
        return db;
    }
    /** 根据mycat schema配置中的对应关系获取对应的db */
    public static String getSchema(Map<String, SchemaConfig> schemaConfigMap, String table) {
        String db = null;
        for (Map.Entry<String,SchemaConfig> entry : schemaConfigMap.entrySet()) {
            if(entry.getValue().getTables().containsKey(table.toUpperCase())) {
                db = entry.getKey();
                break;
            }
        }
        return db;
    }
    public static String parseShowTableSchema(String sql) {
        Matcher ma = pattern.matcher(sql);
        if (ma.matches() && ma.groupCount() >= 5) {
            return ma.group(5);
        }
        return null;
    }

    private static SchemaInfo parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {

        stmt.accept(schemaStatVisitor);
        String key = schemaStatVisitor.getCurrentTable();
        if (key != null && key.contains("`")) {
            key = key.replaceAll("`", "");
        }

        if (key != null) {
            SchemaInfo schemaInfo = new SchemaInfo();
            int pos = key.indexOf(".");
            if (pos > 0) {
                schemaInfo.schema = key.substring(0, pos);
                schemaInfo.table = key.substring(pos + 1);
            } else {
                schemaInfo.table = key;
            }
            return schemaInfo;
        }

        return null;
    }


    public static class SchemaInfo {
        public String table;
        public String schema;

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("SchemaInfo{");
            sb.append("table='").append(table).append('\'');
            sb.append(", schema='").append(schema).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
    //sample：SHOW FULL TABLES FROM information_schema WHERE Tables_in_information_schema LIKE 'KEY_COLUMN_USAGE'
    //注意sql中like后面会有单引号
    private static Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*(['a-zA-Z_0-9\\s]*)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) {
        String sql = "SELECT name, type FROM `mysql`.`proc` as xxxx WHERE Db='base'";
        //   System.out.println(parseSchema(sql));
        sql = "insert into aaa.test(id) values(1)";
        // System.out.println(parseSchema(sql));
        sql = "update updatebase.test set xx=1 ";
        //System.out.println(parseSchema(sql));
        sql = "CREATE TABLE IF not EXISTS  `test` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `sid` bigint(20) DEFAULT NULL,\n" + "  `name` varchar(45) DEFAULT NULL,\n"
                + "  `value` varchar(45) DEFAULT NULL,\n"
                + "  `_slot` int(11) DEFAULT NULL COMMENT '自动迁移算法slot,禁止修改',\n" + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=805781256930734081 DEFAULT CHARSET=utf8";
        System.out.println(parseSchema(sql));
        String pat3 = "SHOW FULL TABLES FROM information_schema WHERE Tables_in_information_schema LIKE 'KEY_COLUMN_USAGE'";
        Matcher ma = pattern.matcher(pat3);
        if (ma.matches()) {
            System.out.println(ma.groupCount());
            System.out.println(ma.group(5));
        }


    }
}
