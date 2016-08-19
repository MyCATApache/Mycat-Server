package io.mycat.sqlexecute;


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

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try{
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            //STEP 4: Execute a query
            System.out.println("Creating statement...");
            String sql = "UPDATE test set sid=?,asf=?  WHERE id<20";
//            stmt = conn.prepareStatement(sql);
//
//            //Bind values into the parameters.
//            stmt.setInt(1, 35);  // This would set age
//            stmt.setString(2, "aaaa"); // This would set ID
//
//            // Let us update age of the record with ID = 102;
//            int rows = stmt.executeUpdate();
//            System.out.println("Rows impacted : " + rows );

            // Let us select all the records and display them.
            sql = "SELECT *  FROM test  where id<?";

            stmt=conn.prepareStatement(sql)   ;
            stmt.setInt(1,8);
            ResultSet rs = stmt.executeQuery();
           // print("",rs);
            //STEP 5: Extract data from result set
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
            //STEP 6: Clean-up environment
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

