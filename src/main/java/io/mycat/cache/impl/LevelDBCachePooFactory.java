package io.mycat.cache.impl;


import io.mycat.cache.CachePool;
import io.mycat.cache.CachePoolFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

public class LevelDBCachePooFactory extends CachePoolFactory {

	@Override
	public CachePool createCachePool(String poolName, int cacheSize,
			int expireSeconds) {
  	  Options options = new Options();
  	  options.cacheSize(cacheSize * 1048576);//cacheSize M 大小
  	  options.createIfMissing(true);
  	  DB db =null;
  	  try {
  		 db=factory.open(new File("leveldb\\"+poolName), options);
  	    // Use the db in here....
  	  } catch (Exception e) {
  	    // Make sure you close the db to shutdown the 
  	    // database and avoid resource leaks.
  	   // db.close();
  	  }
	  return new LevelDBPool(poolName,db,cacheSize);
	}

}
