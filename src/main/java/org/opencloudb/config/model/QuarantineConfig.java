/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.config.model;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.alibaba.druid.wall.WallConfig;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;

import demo.catlets.ZkCreate;

/**
 * 隔离区配置定义
 * 
 * @author songwie
 */
public final class QuarantineConfig {
    private Map<String, List<UserConfig>> whitehost;
    private List<String> blacklist;
    private boolean check = false;
    
    private WallConfig wallConfig = new WallConfig();
     
    private static WallProvider provider ;
    
    public QuarantineConfig() { }
    
    public void init(){
    	if(check){
    		provider = new MySqlWallProvider(wallConfig);
    		provider.setBlackListEnable(true);
    	}
    }
    
    public WallProvider getWallProvider(){
    	return provider;
    }

	public Map<String, List<UserConfig>> getWhitehost() {
		return this.whitehost;
	}
	public void setWhitehost(Map<String, List<UserConfig>> whitehost) {
		this.whitehost = whitehost;
	}
	
	public boolean addWhitehost(String host, List<UserConfig> Users) {
		if (existsHost(host)){
			return false;	
		}
		else {
		 this.whitehost.put(host, Users);
		 return true;
		}
	}
	
	public List<String> getBlacklist() {
		return this.blacklist;
	}
	public void setBlacklist(List<String> blacklist) {
		this.blacklist = blacklist;
	}
	
	public WallProvider getProvider() {
		return provider;
	}

	public boolean existsHost(String host) {
		return this.whitehost==null ? false : whitehost.get(host)!=null ;
	}
	public boolean canConnect(String host,String user) {
		if(whitehost==null || whitehost.size()==0){
			MycatConfig config = MycatServer.getInstance().getConfig();
			Map<String, UserConfig> users = config.getUsers();
			return users.containsKey(user);
		}else{
			List<UserConfig> list = whitehost.get(host);
			if(list==null){
				return false;
			}
			for(UserConfig userConfig : list){
				if(userConfig.getName().equals(user)){
					return true;
				}
			}
		}
		return false ;
	}
	
	public static void setProvider(WallProvider provider) {
		QuarantineConfig.provider = provider;
	}

	public void setWallConfig(WallConfig wallConfig) {
		this.wallConfig = wallConfig;
		
	}

	public boolean isCheck() {
		return this.check;
	}

	public void setCheck(boolean check) {
		this.check = check;
	}

	public WallConfig getWallConfig() {
		return this.wallConfig;
	}
	
	public synchronized static void updateToFile(String host, List<UserConfig> userConfigs) throws Exception{
		String filename = SystemConfig.getHomePath()+ File.separator +"conf"+ File.separator +"server.xml";
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xmldoc = builder.parse(filename);
        NodeList whitehosts = xmldoc.getElementsByTagName("whitehost");
        Element whitehost = (Element) whitehosts.item(0);
        
        for(UserConfig userConfig : userConfigs){
        	String user = userConfig.getName();
        	Element hostEle = xmldoc.createElement("host");
        	hostEle.setAttribute("host", host);
        	hostEle.setAttribute("user", user);

        	whitehost.appendChild(hostEle);
        }
        
             
        TransformerFactory factory2 = TransformerFactory.newInstance();
        Transformer former = factory2.newTransformer();
        former.transform(new DOMSource(xmldoc), new StreamResult(new File(filename)));

	}
	
	public static void main(String[] args) throws Exception {
        List<UserConfig> userConfigs = new ArrayList<UserConfig>();
        UserConfig user = new UserConfig();
        user.setName("mycat");
        userConfigs.add(user);
		updateToFile("127.0.0.6",userConfigs);
	}
	
	
}