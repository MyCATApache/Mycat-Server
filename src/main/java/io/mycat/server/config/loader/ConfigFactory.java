package io.mycat.server.config.loader;

import io.mycat.server.config.ConfigException;
import io.mycat.server.config.ConfigUtil;
import io.mycat.server.config.cluster.ClusterSync;
import io.mycat.server.config.cluster.DatabaseClusterSync;
import io.mycat.server.config.cluster.LocalClusterSync;
import io.mycat.server.config.cluster.ZookeeperClusterSync;
import io.mycat.server.config.loader.zkloader.ZookeeperLoader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class ConfigFactory {
    private final static String ZOOKEEPER = "zookeeper";
    private final static String DATABASE = "jdbc";
    private final static String LOCAL = "local";
    private static String registryAddress = null ;


	public static ConfigLoader instanceLoader(){
		ConfigFactory.load();
		if(registryAddress==null){
        	return ConfigFactory.instanceLocalLoader();
		}else if(registryAddress.startsWith(ZOOKEEPER)){
        	return ConfigFactory.instanceZkLoader();
		}else if(registryAddress.startsWith(DATABASE)){
        	return ConfigFactory.instanceDBLoader();
		}else if(registryAddress.startsWith(LOCAL)){
        	return ConfigFactory.instanceLocalLoader();
		}else {
			throw new ConfigException("regist center: "+ registryAddress +" is not supported,only zk,database ");
		}
	}
	public static ClusterSync instanceCluster(){
		ConfigFactory.load();
		if(registryAddress==null){
        	return new LocalClusterSync();
		}else if(registryAddress.startsWith(ZOOKEEPER)){
        	return new ZookeeperClusterSync();
		}else if(registryAddress.startsWith(DATABASE)){
        	return new DatabaseClusterSync();
		}else if(registryAddress.startsWith(LOCAL)){
        	return new LocalClusterSync();
		}else {
			throw new ConfigException("regist center: "+ registryAddress +" is not supported,only zk,database ");
		}
	}
	private static void load() {
        try {
            Element root = LocalLoader.getRoot();
            registryAddress = loadSystem(root);
        } catch (ConfigException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigException(e);
        }
    }
	private static String loadSystem(Element root) throws IllegalAccessException, InvocationTargetException {
        NodeList serverList = root.getElementsByTagName("server-config");
        Element systemEle = (Element) serverList.item(0);
        NodeList list = systemEle.getElementsByTagName("system");
        for (int i = 0, n = list.getLength(); i < n; i++) {
            Node node = list.item(i);
            if (node instanceof Element) {
                Map<String, Object> props = ConfigUtil.loadElements((Element) node);
                if(props.containsKey("registryAddress")){
                	return (String) props.get("registryAddress");
                }
            }
        }
        return null;
    }
	private static ConfigLoader instanceDBLoader() {
		return null;
	}
	private static ConfigLoader instanceZkLoader(){
        ZookeeperLoader zookeeperLoader = new ZookeeperLoader();
        zookeeperLoader.initConfig();
        return null;
	}

	private static ConfigLoader instanceLocalLoader(){
		return new LocalLoader();
	}
}
