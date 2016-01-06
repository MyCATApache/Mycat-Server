package io.mycat.backend.postgresql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.LoggerFactory;

public class EchoServer {
	public static class EchoThor extends Thread {
		org.slf4j.Logger logger = LoggerFactory.getLogger(EchoThor.class);

		private Socket socket;

		public EchoThor(Socket socket) {
			this.socket = socket;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		@Override
		public void run() {
			try {
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				for (int i = 0; i < 1000000; i++) {
					byte[] b = new byte[1024 * 5];
					in.read(b);
					logger.info("读到了数据....");
					out.write("测试一下".getBytes());
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public static void main(String[] args) {
		try {
			@SuppressWarnings("resource")
			ServerSocket serverSocket = new ServerSocket(5210);

			for (;;) {
				Socket socket = serverSocket.accept();
				new EchoThor(socket).start();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
