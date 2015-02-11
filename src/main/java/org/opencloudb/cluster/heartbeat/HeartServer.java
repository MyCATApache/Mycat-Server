package org.opencloudb.cluster.heartbeat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;



public class HeartServer {

	private final static String BEARSTR = "select 1";
	private static Connection conn = null;

	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) { }
	}

	public static boolean beat(String url,String user,String password){

		try {
			conn = DriverManager.getConnection(url, user, password);

			PreparedStatement prepare = conn.prepareStatement(BEARSTR);
			ResultSet result = prepare.executeQuery();
			if(result.next()){
				return true;
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return false;
		}

		return true;
	}

}
