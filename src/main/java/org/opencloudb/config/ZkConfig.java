package org.opencloudb.config;

import demo.catlets.ZkCreate;

/**
 * Created by StoneGod on 2015/11/23.
 */
public class ZkConfig {
	private ZkConfig(){}
	
	public synchronized static ZkConfig instance(){
		return new ZkConfig();
	}
    public void initZk(){
    	ZkCreate.main(null);
    }
}
