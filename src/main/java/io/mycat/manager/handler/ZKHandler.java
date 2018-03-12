package io.mycat.manager.handler;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.ErrorCode;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.console.ZkNofiflyCfg;
import io.mycat.config.loader.zkprocess.zktoxml.ZktoXmlMain;
import io.mycat.manager.ManagerConnection;
import io.mycat.manager.response.ReloadZktoXml;
import io.mycat.util.ZKUtils;

/**
 * zookeeper 实现动态配置
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 23:35 2016/5/7
 */
public class ZKHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZKHandler.class);

    /**
     * 直接从zk拉所有配置，然后本地执行reload_all
     */
    public static final String RELOAD_FROM_ZK = "zk reload_from_zk";

    /**
     * 强制所有节点操作
     */
    private static final String RELOAD_ALL = "all";

    /**
     * 命令节点信息
     */
    public static final String ZK_NODE_PATH = "command";

    public static void handle(String stmt, ManagerConnection c, int offset) {
        String command = stmt.toLowerCase();
        // 检查当前的命令是否为zk reload_from_zk
        if (RELOAD_FROM_ZK.equals(command)) {
            // 调用zktoxml操作
            try {
                // 通知所有节点进行数据更新
                ZktoXmlMain.ZKLISTENER.notifly(ZkNofiflyCfg.ZK_NOTIFLY_LOAD_ALL.getKey());

                // 执行重新加载本地配制信息
                ReloadHandler.handle("RELOAD @@config_all", c, 7 >>> 8);

                offset += RELOAD_FROM_ZK.length();

                ReloadZktoXml.execute(c, "zk reload success ");
            } catch (Exception e) {
                LOGGER.error("ZKHandler loadZktoFile exception", e);
                c.writeErrMessage(ErrorCode.ER_YES, "zk command send error,command is :" + command);
            }
        } else {
            String[] matchKeys = stmt.split("\\s+");

            if (null != matchKeys && matchKeys.length > 2) {
                // 取得所有配制的节点信息
                String key = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTER_NODES);

                String[] myidArray = key.split(",");

                String idkeys = matchKeys[1].toLowerCase();

                // 发送的命令信息
                StringBuilder commandMsg = new StringBuilder();

                for (int i = 2; i < matchKeys.length; i++) {
                    if (i == matchKeys.length - 1) {
                        commandMsg.append(matchKeys[i]);
                    } else {
                        commandMsg.append(matchKeys[i]).append(" ");
                    }
                }

                // 命令的形式为zk all reload_from_zk
                // 进行第二个匹配，检查是否为所有节点更新
                if (RELOAD_ALL.equals(idkeys)) {
                    // 按所有id，将把所有的节点都更新
                    try {
                        // 将所有指令发送至服务器
                        for (String myid : myidArray) {
                            sendZkCommand(myid, commandMsg.toString());
                        }

                        ReloadZktoXml.execute(c, "zk reload " + matchKeys[1] + " success ");
                    } catch (Exception e) {
                        c.writeErrMessage(ErrorCode.ER_YES, "zk command send error");
                    }
                }
                // 如果不是所有节点，则检查是否能匹配上单独的节点
                else {
                    for (String myid : myidArray) {
                        if (myid.equals(idkeys)) {
                            try {
                                sendZkCommand(myid, commandMsg.toString());

                                ReloadZktoXml.execute(c, "zk reload " + matchKeys[1] + " success ");
                            } catch (Exception e) {
                                c.writeErrMessage(ErrorCode.ER_YES, "zk command send error,myid :" + myid);
                            }

                            break;
                        }
                    }
                }

            } else {
                c.writeErrMessage(ErrorCode.ER_YES, "zk command is error");
            }
        }
    }

    /**
     * 向节点发送命令
     * @param myId 节点的id信息
     * @param command 命令内容 
     * @throws Exception 异常信息
     */
    private static void sendZkCommand(String myId, String command) throws Exception {
        CuratorFramework zkConn = ZKUtils.getConnection();

        String basePath = ZKUtils.getZKBasePath();

        String nodePath = ZKPaths.makePath(basePath, ZK_NODE_PATH + "/" + myId);

        Stat stat;
        try {
            stat = zkConn.checkExists().forPath(nodePath);

            if (null == stat) {
                // 进行目录的创建操作
                ZKPaths.mkdirs(zkConn.getZookeeperClient().getZooKeeper(), nodePath);
            }
            // 设置节点信息
            zkConn.setData().inBackground().forPath(nodePath, command.getBytes());
        } catch (Exception e) {
            LOGGER.error("ZKHandler sendZkCommand exception", e);
            throw e;
        }

    }
}
