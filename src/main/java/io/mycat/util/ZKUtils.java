package io.mycat.util;

import io.mycat.MycatServer;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);
    static CuratorFramework curatorFramework = null;
    static ConcurrentMap<String, PathChildrenCache> watchMap = new ConcurrentHashMap<>();

    static {
        curatorFramework = createConnection();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (curatorFramework != null)
                    curatorFramework.close();
                watchMap.clear();
            }
        }));
    }

    public static String getZKBasePath() {
        String clasterID = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);

        return "/mycat/" + clasterID + "/";
    }

    public static CuratorFramework getConnection() {
        return curatorFramework;
    }

    private static CuratorFramework createConnection() {
        String url = ZkConfig.getInstance().getZkURL();

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));

        // start connection
        curatorFramework.start();
        // wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        // fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }

    public static void closeWatch(List<String> watchs) {
        for (String watch : watchs) {
            closeWatch(watch);
        }
    }

    public static void closeWatch(String path) {
        PathChildrenCache childrenCache = watchMap.get(path);
        if (childrenCache != null) {
            try {
                childrenCache.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void addChildPathCache(String path, PathChildrenCacheListener listener) {
        NameableExecutor businessExecutor = MycatServer.getInstance().getBusinessExecutor();
        ExecutorService executor = businessExecutor == null ? Executors.newFixedThreadPool(5) : businessExecutor;

        try {
            /**
             * 监听子节点的变化情况
             */
            final PathChildrenCache childrenCache = new PathChildrenCache(getConnection(), path, true);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(listener, executor);
            watchMap.put(path, childrenCache);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    

  //写数据到某个路径底下
  	public static boolean writeProperty( String path, Map<String, String> propertyMap) throws Exception {
  		// save to  zk
  		//try {
  			CuratorFramework client = ZKUtils.getConnection();
  			//lock.acquire(30,TimeUnit.SECONDS)   ;			
  			Properties properties=new Properties();
  			ByteArrayOutputStream out=new ByteArrayOutputStream();
  	
  			if(client.checkExists().forPath(path)==null) {
  				for(String key : propertyMap.keySet()){
  					properties.setProperty(key, propertyMap.get(key));
  				}
  				properties.store(out, "add");				
  				client.create().creatingParentsIfNeeded().forPath(path,out.toByteArray());
  				return true;
  			} else{
  				byte[] data = client.getData().forPath(path);
  				properties.load(new ByteArrayInputStream(data));
  				boolean isUpdate = false;
  				for(String key : propertyMap.keySet()){
  					String value = propertyMap.get(key);
  					if(!String.valueOf(value).equals(properties.getProperty(key))) {
  						 properties.setProperty(key, String.valueOf(value));
  						 isUpdate =  true;
  					 }
  				}
  				 properties.store(out, "update");
  	
  				//数据有进行更新
  				if(isUpdate){
  					 client.setData().forPath(path, out.toByteArray());
  					 return true;
  				}
  				return false;
  				 
  			}
  	
  		//}finally {
  		//	lock.release();
  		//}
  	}
	public static void createPath(String path, String data) {
		//这边应该将结果写入到 dnindex.properties
		CuratorFramework client = ZKUtils.getConnection();

		try {
			if(client.checkExists().forPath(path) == null) {
				client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
			} else {
				client.setData().forPath(path, data.getBytes());
			}
		} catch (Exception e) {
			System.out.println("放置数据失败");
			e.printStackTrace();
		}
	}
     public static String getDnIndexPath(){    	
    	 return ZKUtils.getZKBasePath() + "bindata/dnindex.properties";
     } 


    
}