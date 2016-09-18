package io.mycat.config.loader.zkprocess.comm;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;

/**
 * 进行zookeeper操作的监控器器父类信息
 * 
 * @author liujun
 * 
 * @date 2015年2月4日
 * @vsersion 0.0.1
 */
public class ZookeeperProcessListen {

    /**
     * 日志
    * @字段说明 LOGGER
    */
    private static final Logger lOG = LoggerFactory.getLogger(ZookeeperProcessListen.class);

    /**
     * 所有更新缓存操作的集合
     */
    private Map<String, NotiflyService> LISTEN_CACHE = new ConcurrentSkipListMap<String, NotiflyService>();

    /**
     * 基本路径信息
    * @字段说明 basePath
    */
    private String basePath;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * 添加缓存更新操作
     * 
     * @param key
     * @param cacheNotiflySercie
     */
    public void addListen(String key, NotiflyService cacheNotiflySercie) {
        LISTEN_CACHE.put(key, cacheNotiflySercie);
    }

    /**
     * 进行缓存更新通知
     * 
     * @param key
     *            缓存模块的key
     * @return true 当前缓存模块数据更新成功，false，当前缓存数据更新失败
     */
    public boolean notifly(String key) {
        boolean result = false;

        if (null != key && !"".equals(key)) {

            // 进行配制加载所有
            if (ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey().equals(key)) {
                this.notiflyAll();
            }
            // 如果是具体的单独更新，则进行单业务的业务刷新
            else {
                // 取得具体的业务监听信息
                NotiflyService cacheService = LISTEN_CACHE.get(key);

                if (null != cacheService) {
                    try {
                        result = cacheService.notiflyProcess();
                    } catch (Exception e) {
                        e.printStackTrace();
                        lOG.error("ZookeeperProcessListen notifly key :" + key + " error:Exception info:", e);
                    }
                }
            }
        }

        return result;
    }

    /**
     * 进行通知所有缓存进行更新操作
     */
    private void notiflyAll() {

        Iterator<Entry<String, NotiflyService>> notiflyIter = LISTEN_CACHE.entrySet().iterator();

        Entry<String, NotiflyService> item = null;

        while (notiflyIter.hasNext()) {
            item = notiflyIter.next();

            // 进行缓存更新通知操作
            if (null != item.getValue()) {
                try {
                    item.getValue().notiflyProcess();
                } catch (Exception e) {
                    lOG.error("ZookeeperProcessListen notiflyAll key :" + item.getKey() + ";value " + item.getValue()
                            + ";error:Exception info:", e);
                }
            }
        }
    }

}
