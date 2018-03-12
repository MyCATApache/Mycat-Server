package demo.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author mycat
 *
 */
public class TestClass1 {

    public static void main( String args[] ) throws SQLException , ClassNotFoundException {
        String jdbcdriver="com.mysql.jdbc.Driver";
        String jdbcurl="jdbc:mysql://127.0.0.1:8066/TESTDB?useUnicode=true&characterEncoding=utf-8";
        String username="test";
        String password="test";
        System.out.println("开始连接mysql:"+jdbcurl);
        Class.forName(jdbcdriver);
        Connection c = DriverManager.getConnection(jdbcurl,username,password); 
        Statement st = c.createStatement();
        print( "test jdbc " , st.executeQuery("select count(*) from travelrecord ")); 
        System.out.println("OK......");
    }

         static void print( String name , ResultSet res )
                    throws SQLException {
                    System.out.println( name);
                    ResultSetMetaData meta=res.getMetaData();                       
                    //System.out.println( "\t"+res.getRow()+"条记录");
                    String  str="";
                    for(int i=1;i<=meta.getColumnCount();i++){
                        str+=meta.getColumnName(i)+"   ";
                        //System.out.println( meta.getColumnName(i)+"   ");
                    }
                    System.out.println("\t"+str);
                    str="";
                    while ( res.next() ){
                        for(int i=1;i<=meta.getColumnCount();i++){  
                            str+= res.getString(i)+"   ";       
                            } 
                        System.out.println("\t"+str);
                        str="";
                    }
                }
}
