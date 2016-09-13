package io.mycat.config.loader.zookeeper.create;

/**
 * zk服务配制信息的创建 
* 源文件名：ZkCreate.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月12日
* 修改作者：liujun
* 修改日期：2016年9月12日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class ZkCreate {
    /**
     * zk服务信息
    * @字段说明 zkService
    */
    private static ConfToZkService zkService = new ConfToZkService();

    public static void main(String[] args) {
        String url = null;
        String zkFileName = null;
        if (args != null && args.length > 0) {
            zkFileName = args[0];
            url = args[1];
        }

        zkService.writeToZk(zkFileName, url);

    }

}
