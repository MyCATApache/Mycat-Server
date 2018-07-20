package io.mycat.cache.impl;


import io.mycat.cache.CachePool;
import io.mycat.cache.CacheStatic;
import org.iq80.leveldb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;


public class LevelDBPool implements CachePool {
	private static final Logger LOGGER = LoggerFactory.getLogger(LevelDBPool.class);
	private final DB cache;
	private final CacheStatic cacheStati = new CacheStatic();
    private final String name;
    private final long maxSize;
    
	public LevelDBPool(String name,DB db,long maxSize) {
		this.cache = db;
		this.name=name;
		this.maxSize=maxSize;
		cacheStati.setMaxSize(maxSize);
	}
	@Override
	public void putIfAbsent(Object key, Object value) {
		
		cache.put(toByteArray(key),toByteArray(value));
		cacheStati.incPutTimes();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(name+" add leveldb cache ,key:" + key + " value:" + value);
		}		
	}

	@Override
	public Object get(Object key) {
		
		Object  ob= toObject(cache.get(toByteArray(key)));
		if (ob != null) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(name+" hit cache ,key:" + key);
			}
			cacheStati.incHitTimes();
			return ob;
		} else {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(name+"  miss cache ,key:" + key);
			}
			cacheStati.incAccessTimes();
			return null;
		}
	}

	@Override
	public void clearCache() {
		LOGGER.info("clear cache "+name);
		//cache.delete(key);
		cacheStati.reset();
		//cacheStati.setMemorySize(cache.g);
		
	}

	@Override
	public CacheStatic getCacheStatic() {
		
		/*
		int i=0;		
		try {
		 // DBIterator iterator = cache.iterator();	
		  for(cache.iterator().seekToFirst(); cache.iterator().hasNext(); cache.iterator().next()) {
			  i++;
		  }
		  cache.iterator().close();
		} catch (Exception e) {
			  // Make sure you close the iterator to avoid resource leaks.			  
		}		
		//long[] sizes = cache.getApproximateSizes(new Range(bytes("TESTDB"), bytes("TESTDC")));
		 */
		//cacheStati.setItemSize(cache.getSize());//sizes[0]);//需要修改leveldb的代码
		cacheStati.setItemSize(cacheStati.getPutTimes());
		return cacheStati;
	}

	@Override
	public long getMaxSize() {
		
		return maxSize;
	}
	
    public  byte[] toByteArray (Object obj) {        
        byte[] bytes = null;        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();        
        try {          
            ObjectOutputStream oos = new ObjectOutputStream(bos);           
            oos.writeObject(obj);          
            oos.flush();           
            bytes = bos.toByteArray ();        
            oos.close();           
            bos.close();          
        } catch (IOException ex) {
            LOGGER.error("toByteArrayError", ex);
        }        
        return bytes;      
    }     
         
        
    public  Object toObject (byte[] bytes) {        
        Object obj = null;   
        if ((bytes==null) || (bytes.length<=0)) {
        	return obj;
        }
        try {          
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);          
            ObjectInputStream ois = new ObjectInputStream (bis);          
            obj = ois.readObject();        
            ois.close();     
            bis.close();     
        } catch (IOException ex) {    
            LOGGER.error("toObjectError", ex);
        } catch (ClassNotFoundException ex) {          
            LOGGER.error("toObjectError", ex);
        }        
        return obj;      
    } 

}
