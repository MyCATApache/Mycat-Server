package io.mycat.manager.handler;

import io.mycat.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import io.mycat.manager.ManagerConnection;

/**
 * zookeeper 实现动态配置
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 23:35 2016/5/7
 */
public class ZKHandler {

    /**
     * 直接从zk拉所有配置，然后本地执行reload_all
     */
    private static final String RELOAD_FROM_ZK = "zk reload_from_zk";

    /**
     * 一个是强制所有mycat reload from zk 一下
     */
    private static final String RELOAD_ALL = "zk reload_all_mycat_from_zk";

    public static void handle(String stmt, ManagerConnection c, int offset) {
        String command = stmt.toLowerCase();
        // 检查当前的命令是否为zk reload_from_zk
        if (RELOAD_FROM_ZK.equals(command)) {
            // 调用zktoxml操作
            try {
                ZktoXmlMain.loadZktoFile();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // 执行重新加载本地配制信息
            ReloadHandler.handle("RELOAD @@CONFIG", c, 7 >>> 8);

            offset += RELOAD_FROM_ZK.length();
        }

        // reload_all_mycat_from_zk

    }
}
