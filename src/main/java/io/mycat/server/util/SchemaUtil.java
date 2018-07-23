package io.mycat.server.util;

import com.alibaba.druid.sql.ast.SQLStatement;
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
 * 逻辑库 数据库工具
 * Created by magicdoom on 2016/1/26.
 */
public class SchemaUtil {
    /**
     * 解析逻辑库 数据库
     * @param sql
     * @return
     */
    public static SchemaInfo parseSchema(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);
        return parseTables(parser.parseStatement(),new MycatSchemaStatVisitor()  );
    }

    /**
     * 检测默认逻辑库 数据库
     * @param sql
     * @param type
     * @return
     */
    public static String detectDefaultDb(String sql, int type) {
        String db=null;
        Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig().getSchemas();
        if(ServerParse.SELECT==type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if ((schemaInfo==null||schemaInfo.table==null)&&!schemaConfigMap.isEmpty()) {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }

            if (schemaInfo != null && schemaInfo.schema != null ) {
                if ( schemaConfigMap.containsKey(schemaInfo.schema) ) {
                    db = schemaInfo.schema;
                    /**
                     * 对 MySQL 自带的元数据库 information_schema 进行返回
                     */
                } else if ( "information_schema".equalsIgnoreCase( schemaInfo.schema ) ) {
                    db = "information_schema";
                }
            }
        } else if(ServerParse.INSERT==type||ServerParse.UPDATE==type||ServerParse.DELETE==type||ServerParse.DDL==type) {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if(schemaInfo!=null&&schemaInfo.schema!=null&&schemaConfigMap.containsKey(schemaInfo.schema)  ) {
                db = schemaInfo.schema;
            }
        } else if((ServerParse.SHOW==type||ServerParse.USE==type||ServerParse.EXPLAIN==type||ServerParse.SET==type
                ||ServerParse.HELP==type||ServerParse.DESCRIBE==type)
                && !schemaConfigMap.isEmpty()) {
            //兼容mysql gui  不填默认database
            db = schemaConfigMap.entrySet().iterator().next().getKey();
        }
        return db;
    }


    public static String parseShowTableSchema(String sql) {
        Matcher ma = pattern.matcher(sql);
        if(ma.matches()&&ma.groupCount()>=5) {
            return  ma.group(5);
        }
        return null;
    }

    /**
     * 解析表
     * @param stmt
     * @param schemaStatVisitor
     * @return
     */
    private static SchemaInfo parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {
        stmt.accept(schemaStatVisitor);
        if(schemaStatVisitor.getTables() == null
                || schemaStatVisitor.getTables().isEmpty()){
            return null;
        }
        // 获取当前表
        String key = schemaStatVisitor.getTables().keySet().iterator().next().getName();// druid 1.1.3 没有这个方法
        if (key != null && key.contains("`")) {
            key = key.replaceAll("`", "");
        }

        if (key != null) {
            SchemaInfo schemaInfo = new SchemaInfo();
            int pos = key.indexOf(".");
            if (pos > 0) {
                schemaInfo.schema = key.substring(0, pos);
                schemaInfo.table = key.substring(pos + 1);
            } else if(schemaStatVisitor.getRepository().getDefaultSchemaName()!=null
                    && schemaStatVisitor.getRepository().getDefaultSchemaName().length()>0){
                schemaInfo.schema = schemaStatVisitor.getRepository().getDefaultSchemaName();
                schemaInfo.table = key;
            }else{
                schemaInfo.table = key;
            }
            return schemaInfo;
        }
        return null;
    }

    /**
     * 逻辑库 数据库 -- 信息
     */
    public static  class SchemaInfo {
        // 逻辑表 数据表
        public String table;
        // 逻辑库 数据库
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

    private  static Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*([a-zA-Z_0-9\\s]*)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args) {
        String sql = "SELECT name, type FROM `mysql`.`proc` as xxxx WHERE Db='base'";
        //   System.out.println(parseSchema(sql));
        sql="insert into aaa.test(id) values(1)" ;
        // System.out.println(parseSchema(sql));
        sql="update updatebase.test set xx=1 " ;
        //System.out.println(parseSchema(sql));
        sql="CREATE TABLE IF not EXISTS  `test` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                + "  `sid` bigint(20) DEFAULT NULL,\n" + "  `name` varchar(45) DEFAULT NULL,\n"
                + "  `value` varchar(45) DEFAULT NULL,\n"
                + "  `_slot` int(11) DEFAULT NULL COMMENT '自动迁移算法slot,禁止修改',\n" + "  PRIMARY KEY (`id`)\n"
                + ") ENGINE=InnoDB AUTO_INCREMENT=805781256930734081 DEFAULT CHARSET=utf8";
        System.out.println(parseSchema(sql));
        String pat3 = "show  full  tables from  base like ";
        Matcher ma = pattern.matcher(pat3);
        if(ma.matches()) {
            System.out.println(ma.groupCount());
            System.out.println(ma.group(5));
        }
    }
}
