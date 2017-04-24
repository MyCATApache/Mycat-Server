package io.mycat.backend.mysql.nio.handler;

public class SecondQueryHandler implements SecondHandler {

	public MiddlerResultHandler middlerResultHandler;
	public SecondQueryHandler(MiddlerResultHandler middlerResultHandler){
		this.middlerResultHandler =  middlerResultHandler;
	}
	@Override
	public void doExecute(String param) {
		// TODO Auto-generated method stub
		
	}
	 

}
