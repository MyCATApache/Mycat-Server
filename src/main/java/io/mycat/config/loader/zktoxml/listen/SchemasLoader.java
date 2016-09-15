package io.mycat.config.loader.zktoxml.listen;

import java.lang.reflect.Type;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;

import com.google.gson.reflect.TypeToken;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zktoxml.comm.XmlProcessBase;
import io.mycat.config.loader.zktoxml.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zktoxml.comm.notiflyService;
import io.mycat.config.loader.zktoxml.entry.Schemas;
import io.mycat.config.loader.zktoxml.entry.schema.datahost.DataHost;
import io.mycat.config.loader.zktoxml.entry.schema.datanode.DataNode;
import io.mycat.config.loader.zktoxml.entry.schema.schema.Schema;
import io.mycat.config.loader.zktoxml.zkProcess.DataInf;
import io.mycat.config.loader.zktoxml.zkProcess.DiretoryInf;
import io.mycat.config.loader.zktoxml.zkProcess.zkdirectry.ZkDataImpl;
import io.mycat.config.loader.zktoxml.zkProcess.zkdirectry.ZkDirectoryImpl;
import io.mycat.config.loader.zktoxml.zkProcess.zkdirectry.ZkDirectoryLoader;

/**
 * 进行schema的文件从zk中加载
* 源文件名：SchemasLoader.java
* 文件版本：1.0.0
* 创建作者：liujun
* 创建日期：2016年9月15日
* 修改作者：liujun
* 修改日期：2016年9月15日
* 文件描述：TODO
* 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
*/
public class SchemasLoader extends ZkDirectoryLoader implements notiflyService {

    /**
     * 当前文件中的zkpath信息 
    * @字段说明 currZkPath
    */
    private final String currZkPath;

    /**
     * xml转换对象信息
    * @字段说明 xmlParse
    */
    private XmlProcessBase xmlParse;

    public SchemasLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator, XmlProcessBase xmlParse) {

        this.setCurator(curator);

        // 获得当前集群的名称
        String schemaPath = zookeeperListen.getBasePath();
        schemaPath = schemaPath + ZookeeperPath.ZK_SEPARATOR.getKey() + ZookeeperPath.FOW_ZK_PATH_SCHEMA.getKey();

        currZkPath = schemaPath;
        // 将当前自己注册为事件接收对象
        zookeeperListen.addListen(schemaPath, this);

        this.xmlParse = xmlParse;

        // 注册转换对象信息
        this.xmlParse.addParseClass(Schemas.class);
    }

    @Override
    public boolean cacheNotifly() throws Exception {
        // 1,将集群schema目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf schemaDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey(), schemaDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) schemaDirectory.getSubordinateInfo().get(0);

        Schemas schemaObj = new Schemas();

        // 得到schema对象的目录信息
        DiretoryInf schemaZkDirectory = this.getZkDirectory(zkDirectory,
                ZookeeperPath.FLOW_ZK_PATH_SCHEMA_SCHEMA.getKey());
        // 将schema转换为json字符串
        StringBuilder toschemajson = new StringBuilder();
        toschemajson.append("[");
        this.toJsonStr(schemaZkDirectory, toschemajson);
        toschemajson.append("]");
        String jsonValue = toschemajson.toString();
        // 指定集合对象属性
        Type typeSchema = new TypeToken<List<Schema>>() {
        }.getType();

        List<Schema> schemaList = this.getGson().fromJson(jsonValue, typeSchema);
        schemaObj.setSchema(schemaList);

        // 将dataNode转换为json字符串
        DiretoryInf dataNodeZk = this.getZkDirectory(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATANODE.getKey());
        StringBuilder dataNodeBuild = new StringBuilder();
        this.toDataNodeJsonStr(dataNodeZk, dataNodeBuild);
        String dataNodeJson = dataNodeBuild.toString();
        // 指定集合对象属性
        Type typeNodeData = new TypeToken<List<DataNode>>() {
        }.getType();
        List<DataNode> dataNodeList = this.getGson().fromJson(dataNodeJson, typeNodeData);
        schemaObj.setDataNode(dataNodeList);

        // 转换dataHost信息

        DiretoryInf dataHostZk = this.getZkDirectory(zkDirectory, ZookeeperPath.FLOW_ZK_PATH_SCHEMA_DATAHOST.getKey());
        StringBuilder dataHostBuild = new StringBuilder();
        dataHostBuild.append("[");
        this.toJsonStr(dataHostZk, dataHostBuild);
        dataHostBuild.append("]");
        String dataHostJson = dataHostBuild.toString();

        // 指定集合对象属性
        Type typedataHost = new TypeToken<List<DataHost>>() {
        }.getType();

        List<DataHost> dataHostList = this.getGson().fromJson(dataHostJson, typedataHost);
        schemaObj.setDataHost(dataHostList);

        String path = SchemasLoader.class.getClassLoader().getResource("io/mycat/config/loader/zktoxml/listen/")
                .getPath();
        path = path.substring(1) + "schema.xml";

        this.xmlParse.parseToXml(schemaObj, path, "schema");

        System.out.println("当前schema对象:" + schemaObj);

        return false;
    }

    /**
     * 通过名称获取路径对象信息
    * 方法描述
    * @param zkDirectory
    * @param name
    * @return
    * @创建日期 2016年9月16日
    */
    private DiretoryInf getZkDirectory(DiretoryInf zkDirectory, String name) {
        List<Object> list = zkDirectory.getSubordinateInfo();

        if (null != list && !list.isEmpty()) {
            for (Object directObj : list) {

                if (directObj instanceof ZkDirectoryImpl) {
                    ZkDirectoryImpl zkDirectoryValue = (ZkDirectoryImpl) directObj;

                    if (name.equals(zkDirectoryValue.getName())) {

                        return zkDirectoryValue;
                    }

                }
            }
        }
        return null;
    }

    /**
     * 带子级的信息转换为json的字符信息，可以为schema以及dataHost转换使用
    * 方法描述
    * @param schemaDirectory
    * @param tojson
    * @创建日期 2016年9月15日
    */
    private void toJsonStr(DiretoryInf schemaDirectory, StringBuilder tojson) {
        List<Object> dirctoryList = schemaDirectory.getSubordinateInfo();

        for (Object object : dirctoryList) {
            // 如果当前为目录节点
            if (object instanceof ZkDirectoryImpl) {
                ZkDirectoryImpl directory = (ZkDirectoryImpl) object;
                String value = directory.getValue();
                tojson.append(value.replaceAll("\\}", ""));
                tojson.append(",");
                this.toJsonStr(directory, tojson);
                tojson.append("}");
            }
            // 如果当前为数据节点
            else if (object instanceof DataInf) {
                ZkDataImpl data = (ZkDataImpl) object;
                tojson.append(data.getName());
                tojson.append(":");
                tojson.append("[");
                tojson.append(data.getValue());
                tojson.append("]");
            }
        }
    }

    /**
     * 将schema的信息转换为json的字符信息
     * 方法描述
     * @param dataNodeZk
     * @param tojson
     * @创建日期 2016年9月15日
     */
    private void toDataNodeJsonStr(DiretoryInf dataNodeZk, StringBuilder tojson) {
        List<Object> dirctoryList = dataNodeZk.getSubordinateInfo();
        tojson.append("[");
        for (int i = 0; i < dirctoryList.size(); i++) {
            Object object = dirctoryList.get(i);
            // 如果当前为数据节点
            if (object instanceof DataInf) {
                ZkDataImpl data = (ZkDataImpl) object;
                tojson.append(data.getValue());
                if (i != dirctoryList.size() - 1) {
                    tojson.append(",");
                }
            }
        }
        tojson.append("]");
    }

    /**
     * 将schema的信息转换为json的字符信息
     * 方法描述
     * @param dataHostZk
     * @param tojson
     * @创建日期 2016年9月15日
     */
    private void toDataHostJson(DiretoryInf dataHostZk, StringBuilder tojson) {
        List<Object> dirctoryList = dataHostZk.getSubordinateInfo();
        tojson.append("[");
        for (int i = 0; i < dirctoryList.size(); i++) {
            Object object = dirctoryList.get(i);
            // 如果当前为数据节点
            if (object instanceof DataInf) {
                ZkDataImpl data = (ZkDataImpl) object;
                tojson.append(data.getValue());
                if (i != dirctoryList.size() - 1) {
                    tojson.append(",");
                }
            }
        }
        tojson.append("]");
    }

}
