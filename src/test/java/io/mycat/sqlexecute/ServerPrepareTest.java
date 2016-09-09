package io.mycat.sqlexecute;


import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.*;

/**
 * 
 * @author CrazyPig
 *
 */
public class ServerPrepareTest {

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:8066/TESTDB?useServerPrepStmts=true";
//    static final String DB_URL = "jdbc:mysql://localhost:8066/TESTDB";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "mysql";
    
    static {
    	try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    /**
     * 测试发送COM_STMT_SEND_LONG_DATA命令
     */
    public static void testComStmtSendLondData() {
    	Connection conn = null;
    	PreparedStatement pstmt = null;
    	try {
			conn = DriverManager.getConnection(DB_URL,USER,PASS);
			pstmt = conn.prepareStatement("insert into hotnews(id, title, content) values(?,?,?)");
			pstmt.setInt(1, 1314);
			pstmt.setString(2, "hotnew");
			pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a content of hotnew".getBytes("UTF-8")));
			pstmt.execute();
			pstmt.close();
    	} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
    }
    
    /**
     * 测试发送COM_STMT_RESET命令
     */
    public static void testComStmtRest() {
    	Connection conn = null;
    	PreparedStatement pstmt = null;
    	try {
			conn = DriverManager.getConnection(DB_URL,USER,PASS);
			pstmt = conn.prepareStatement("insert into hotnews(id, title, content) values(?,?,?)");
			pstmt.setInt(1, 1314);
			pstmt.setString(2, "hotnew");
			pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a content of hotnew".getBytes("UTF-8")));
			pstmt.execute();
			pstmt.clearParameters();
			pstmt.setInt(1, 1315);
			pstmt.setString(2, "hotnew");
			pstmt.setBinaryStream(3, new ByteArrayInputStream("this is a new content of hotnew".getBytes("UTF-8")));
			pstmt.execute();
			pstmt.close();
    	} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
    }
    
    public static void simpleTest() {
    	Connection conn = null;
        PreparedStatement stmt = null;
        try{

            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            System.out.println("Creating statement...");
            String sql = "SELECT *  FROM test  where id<?";

            stmt=conn.prepareStatement(sql)   ;
            stmt.setInt(1,8);
            ResultSet rs = stmt.executeQuery();
            // Extract data from result set
            ResultSetMetaData rsmd = rs.getMetaData();
            
            int colCount = rsmd.getColumnCount();
            for(int i = 1; i <= colCount; i++) {
            	System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
            while(rs.next()){
            	//Display values
            	for(int i = 1; i <= colCount; i++) {
            		System.out.print(rs.getObject(i) + "\t");
            	}
            	System.out.println();
            }
            // Clean-up environment
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }

    public static void main(String[] args) {
    	
    	testComStmtRest();
    	
    }//end main


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

