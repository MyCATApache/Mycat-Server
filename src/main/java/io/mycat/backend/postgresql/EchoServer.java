package io.mycat.backend.postgresql;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class EchoServer {
	public static void main(String[] args) {
		try {
			ServerSocket serverSocket= new ServerSocket(5210);
			 
			for(;;){
				Socket socket = serverSocket.accept();
			
			}		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
