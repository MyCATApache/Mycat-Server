package org.opencloudb.server.util;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.opencloudb.MycatServer;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.parser.druid.MycatSchemaStatVisitor;
import org.opencloudb.server.parser.ServerParse;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by magicdoom on 2016/1/26.
 */
public class SchemaUtil
{
    public static SchemaInfo parseSchema(String sql)
    {
        SQLStatementParser parser = new MySqlStatementParser(sql);
      return parseTables(parser.parseStatement(),new MycatSchemaStatVisitor()  );
    }
    public static String detectDefaultDb(String sql, int type)
    {
        String db=null;
        Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig()
                .getSchemas();
        if(ServerParse.SELECT==type)
        {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if ((schemaInfo==null||schemaInfo.table==null)&&!schemaConfigMap.isEmpty())
            {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }

            if(schemaInfo!=null&&schemaInfo.schema!=null&&schemaConfigMap.containsKey(schemaInfo.schema)  )
                db= schemaInfo.schema;
        }
        else
        if(ServerParse.INSERT==type||ServerParse.UPDATE==type||ServerParse.DELETE==type||ServerParse.DDL==type)
        {
            SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
            if(schemaInfo!=null&&schemaInfo.schema!=null&&schemaConfigMap.containsKey(schemaInfo.schema)  )
                db= schemaInfo.schema;
        }   else
        if(ServerParse.SHOW==type||ServerParse.USE==type||ServerParse.EXPLAIN==type||ServerParse.SET==type
                ||ServerParse.HELP==type||ServerParse.DESCRIBE==type)
        {
            //兼容mysql gui  不填默认database
            if (!schemaConfigMap.isEmpty())
            {
                db = schemaConfigMap.entrySet().iterator().next().getKey();
            }
        }
        return db;
    }


    public static String parseShowTableSchema(String sql)
    {
        Matcher ma = pattern.matcher(sql);
        if(ma.matches()&&ma.groupCount()>=5)
        {
          return  ma.group(5);
        }
        return null;
    }

    private static SchemaInfo parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor)
    {

                stmt.accept(schemaStatVisitor);
                String key = schemaStatVisitor.getCurrentTable();
                if (key != null && key.contains("`"))
                {
                    key = key.replaceAll("`", "");
                }

                if (key != null)
                {
                    SchemaInfo schemaInfo=new SchemaInfo();
                    int pos = key.indexOf(".");
                    if (pos > 0)
                    {
                        schemaInfo.schema=key.substring(0,pos);
                        schemaInfo.table=key.substring(pos+1);
                    }  else
                    {
                        schemaInfo.table=key;
                    }
                    return schemaInfo;
                }

        return null;
    }


    public static  class SchemaInfo
    {
        public    String table;
        public    String schema;

        @Override
        public String toString()
        {
            final StringBuffer sb = new StringBuffer("SchemaInfo{");
            sb.append("table='").append(table).append('\'');
            sb.append(", schema='").append(schema).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

private  static     Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(FULL)*\\s*(TABLES)\\s+(FROM)\\s+([a-zA-Z_0-9]+)\\s*([a-zA-Z_0-9\\s]*)", Pattern.CASE_INSENSITIVE);

    public static void main(String[] args)
    {
        String sql = "SELECT name, type FROM `mysql`.`proc` as xxxx WHERE Db='base'";
     //   System.out.println(parseSchema(sql));
        sql="insert into aaa.test(id) values(1)" ;
       // System.out.println(parseSchema(sql));
        sql="update updatebase.test set xx=1 " ;
        //System.out.println(parseSchema(sql));

        String pat3 = "show  full  tables from  base like ";
        Matcher ma = pattern.matcher(pat3);
        if(ma.matches())
        {
            System.out.println(ma.groupCount());
            System.out.println(ma.group(5));
        }



    }
}
