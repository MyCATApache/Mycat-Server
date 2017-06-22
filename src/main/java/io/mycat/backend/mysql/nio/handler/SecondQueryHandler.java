package io.mycat.backend.mysql.nio.handler;

import java.util.List;

public class SecondQueryHandler implements SecondHandler {

	public MiddlerResultHandler middlerResultHandler;
	public SecondQueryHandler(MiddlerResultHandler middlerResultHandler){
		this.middlerResultHandler =  middlerResultHandler;
	}

	@Override
	public void doExecute(List params) {
		// TODO Auto-generated method stub
		
	}
	 

}
