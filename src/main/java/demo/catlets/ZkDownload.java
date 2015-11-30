package demo.catlets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.QName;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.opencloudb.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * aStoneGod 2015.11
 */
public class ZkDownload {
    private static final String ZK_CONFIG_FILE_NAME = "/zk-create.yaml";
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkDownload.class);

    private static final String SERVER_CONFIG_DIRECTORY = "server-config";
    private static final String DATANODE_CONFIG_DIRECTORY = "datanode-config";
    private static final String RULE_CONFIG_DIRECTORY = "rule-config";
    private static final String SEQUENCE_CONFIG_DIRECTORY = "sequence-config";
    private static final String SCHEMA_CONFIG_DIRECTORY = "schema-config";
    private static final String DATAHOST_CONFIG_DIRECTORY = "datahost-config";
    private static final String MYSQLREP_CONFIG_DIRECTORY = "mysqlrep-config";


    private static final String CONFIG_ZONE_KEY = "zkZone";
    private static final String CONFIG_URL_KEY = "zkUrl";
    private static final String CONFIG_CLUSTER_KEY = "zkClu";
    private static final String CONFIG_MYCAT_ID = "zkID";
    private static final String CONFIG_SYSTEM_KEY = "system";
    private static final String CONFIG_USER_KEY = "user";
    private static final String CONFIG_DATANODE_KEY = "datanode";
    private static final String CONFIG_RULE_KEY = "rule";
    private static final String CONFIG_SEQUENCE_KEY = "sequence";
    private static final String CONFIG_SCHEMA_KEY = "schema";
    private static final String CONFIG_DATAHOST_KEY = "datahost";
    private static final String CONFIG_MYSQLREP_KEY = "mysqlrep";



    private static String CLU_PARENT_PATH;
    private static String ZONE_PARENT_PATH;
    private static String SERVER_PARENT_PATH;


    private static CuratorFramework framework;
    private static Map<String, Object> zkConfig;



    public static void main(String[] args) throws Exception {
        init();
    }

    public static boolean init()  {
        LOGGER.info("start zkdownload to local xml");
        zkConfig = loadZkConfig();
        
        ZONE_PARENT_PATH = ZKPaths.makePath("/", String.valueOf(zkConfig.get(CONFIG_ZONE_KEY)));
        CLU_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)));
        SERVER_PARENT_PATH = ZKPaths.makePath(ZONE_PARENT_PATH + "/", String.valueOf(zkConfig.get(CONFIG_CLUSTER_KEY)+"/"+String.valueOf(zkConfig.get(CONFIG_MYCAT_ID))));
        LOGGER.trace("parent path is {}", CLU_PARENT_PATH);
        framework = createConnection((String) zkConfig.get(CONFIG_URL_KEY));
        try {
        	boolean exitsZk = isHavingConfig();
        	if(exitsZk){
        		List<Map<String, JSONObject>> listDataNode = getDatanodeConfig(DATANODE_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listDataHost = getDataHostNodeConfig(CLU_PARENT_PATH,DATAHOST_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listServer = getServerNodeConfig(SERVER_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listSchema = getSchemaConfig(SCHEMA_CONFIG_DIRECTORY);
                //List<Map<String,JSONObject>> listSequence  = getSequenceNodeConfig(SEQUENCE_CONFIG_DIRECTORY);
                List<Map<String, JSONObject>> listRule = getServerNodeConfig(RULE_CONFIG_DIRECTORY);

                //生成SERVER XML
                processServerDocument(listServer);

                //生成SCHEMA XML
                processSchemaDocument(listSchema);

                //生成RULE XML
                processRuleDocument(listRule);
        	}else{
        		return false;
        	}
        }catch (Exception e) {
        	LOGGER.warn("start zkdownload to local error,",e);
        }
        
        return true;
    }

    public static Set<Map<String,JSONObject>> getMysqlRep(List<Map<String, JSONObject>> listMysqlRep,String trepid) throws Exception {
        Set<Map<String, JSONObject>> set = new HashSet<>();
        String[] repids = trepid.split(",");
        for (String repid : repids){
            for (int i=0; i<listMysqlRep.size();i++){
                String datahostName = listMysqlRep.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
                if (datahostName.contains(repid))
                    set.add(listMysqlRep.get(i));
            }
        }
        return set;
    }

    public static void processMysqlRepDocument(Element serverElement,List<Map<String,JSONObject>> mapList) throws Exception {

        for (int i=0;i<mapList.size();i++){
            int subLength = CLU_PARENT_PATH.length()+DATAHOST_CONFIG_DIRECTORY.length()+2;
            int repLength = ZONE_PARENT_PATH.length()+MYSQLREP_CONFIG_DIRECTORY.length()+2;
            String datahostName = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            if (!datahostName.substring(subLength,datahostName.length()).contains("/")){
                String key =datahostName.substring(subLength,datahostName.length());
                JSONObject jsonObject = mapList.get(i).get(datahostName);
                Element dataHost = serverElement.addElement("dataHost");
                if (!key.isEmpty()){
                    Element datahost = dataHost.addAttribute("name", key);
                    if (jsonObject.containsKey("writetype"))
                        datahost.addAttribute("writeType",jsonObject.get("writetype").toString());
                    if (jsonObject.containsKey("switchType"))
                        datahost.addAttribute("switchType",jsonObject.get("switchType").toString());
                    if (jsonObject.containsKey("slaveThreshold"))
                        datahost.addAttribute("slaveThreshold",jsonObject.get("slaveThreshold").toString());
                    if (jsonObject.containsKey("balance"))
                        datahost.addAttribute("balance",jsonObject.get("balance").toString());
                    if (jsonObject.containsKey("dbtype"))
                        datahost.addAttribute("dbType",jsonObject.get("dbtype").toString());
                    if (jsonObject.containsKey("maxcon"))
                        datahost.addAttribute("maxCon",jsonObject.get("maxcon").toString());
                    if (jsonObject.containsKey("mincon"))
                        datahost.addAttribute("minCon",jsonObject.get("mincon").toString());
                    if (jsonObject.containsKey("dbDriver"))
                        datahost.addAttribute("dbDriver",jsonObject.get("dbDriver").toString());
                    if (jsonObject.containsKey("heartbeatSQL")){
                        Element  heartbeatSQL = dataHost.addElement("heartbeat");
                        heartbeatSQL.setText(jsonObject.get("heartbeatSQL").toString());
                    }

                    String repid = jsonObject.get("repid").toString();
                    List<Map<String, JSONObject>> listMysqlRep = getDataHostNodeConfig(ZONE_PARENT_PATH,MYSQLREP_CONFIG_DIRECTORY);
                    Set<Map<String,JSONObject>> datahostSet = getMysqlRep(listMysqlRep,repid);
                    Iterator<Map<String,JSONObject>> it = datahostSet.iterator();
                    //处理WriteHost
                    for (Map<String,JSONObject> wdh : datahostSet) {
                        String host = wdh.keySet().toString().replace("[", "").replace("]", "").trim();
                        String temp = host.substring(repLength, host.length());
                        if (temp.contains("/")&&!temp.contains("readHost")) {
                            String currepid = temp.substring(0,temp.indexOf("/"));
                            String childHost = temp.substring(temp.indexOf("/")+1, temp.length());
                            JSONObject childJsonObject = wdh.get(host);
                            Element writeHost = dataHost.addElement("writeHost");
                            if (childJsonObject.containsKey("host"))
                                writeHost.addAttribute("host", childJsonObject.get("host").toString());
                            if (childJsonObject.containsKey("url"))
                                writeHost.addAttribute("url", childJsonObject.get("url").toString());
                            if (childJsonObject.containsKey("user"))
                                writeHost.addAttribute("user", childJsonObject.get("user").toString());
                            if (childJsonObject.containsKey("password"))
                                writeHost.addAttribute("password", childJsonObject.get("password").toString());
                            //处理readHost
                            for (Map<String,JSONObject> rdh : datahostSet) {
                                String readhost = rdh.keySet().toString().replace("[", "").replace("]", "").trim();
                                if (readhost.equals(host)||readhost.compareTo(host)<=0||!readhost.contains(currepid))
                                    continue;
                                String tempread = readhost.substring(host.length()-childHost.length(), readhost.length());
                                if (tempread.contains("/") && tempread.contains(childHost)) {
                                    String readHost = tempread.substring(childHost.length() + 1, tempread.length());
                                    //System.out.println("readHost:" + readHost);
                                    JSONObject readJsonObject = rdh.get(readhost);
                                    Element readHostEl = writeHost.addElement("readHost");
                                    if (readJsonObject.containsKey("host"))
                                        readHostEl.addAttribute("host", readJsonObject.get("host").toString());
                                    if (readJsonObject.containsKey("url"))
                                        readHostEl.addAttribute("url", readJsonObject.get("url").toString());
                                    if (readJsonObject.containsKey("user"))
                                        readHostEl.addAttribute("user", readJsonObject.get("user").toString());
                                    if (readJsonObject.containsKey("password"))
                                        readHostEl.addAttribute("password", readJsonObject.get("password").toString());
                                }
                            }
                        }
                    }
                }
            }
        }
       // json2XmlFile(document,"mysqlrep.xml");
    }
    
    //config txt file
    public static void conf2File(String fileName,String config) {
        BufferedWriter fw = null;
        try {
            String filePath = SystemConfig.getHomePath()+"/src/main/resources/";
            File file = new File(filePath+fileName);
            //System.out.println(filePath);
            file.delete();
            file.createNewFile();
            fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8")); // 指定编码格式，以免读取时中文字符异常
            String[] configs = config.split(",");
            for (String con:configs){
                fw.write(con);
                fw.newLine();
            }
            fw.flush(); // 全部写入缓存中的内容
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //zk Config
    public static boolean isHavingConfig()throws Exception {
        String nodePath = CLU_PARENT_PATH ;
        LOGGER.trace("child path is {}", nodePath);
        List list = null;
        try {
            list  = framework.getChildren().forPath(nodePath);
		} catch(NoNodeException e){
        	LOGGER.warn("remote zk center not exists node :" + nodePath  );
        	return false;
        }
        if(list!=null && list.size()>0){
        	return true;
        }

        return false;
    }
    
    //Datanode Config
    public static List<Map<String,JSONObject>> getDatanodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        LOGGER.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getDatanodeConfig(listServer, nodePath);
        return listServer;
    }

    //Datanode Child Config
    private static List<Map<String,JSONObject>> getDatanodeConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {

        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                }else {
                    String childPath1 = childPath + "/" + leaf;
                    getServerChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }

    //Schema Config
    public static List<Map<String,JSONObject>> getSchemaConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        LOGGER.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getSchemaChildConfig(listServer, nodePath);
        return listServer;
    }

    //Schema Child Config
    private static List<Map<String,JSONObject>> getSchemaChildConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {
        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        for (int i = 0; i < nodeSize; i++) {
            String leaf = iterator.next();
            String childPath1 = childPath + "/" + leaf;
            listServer=getConfigData(listServer, childPath1);
            getSchemaChildConfig(listServer, childPath1);
        }
        return listServer;
    }

    //sequence Config
    public static List<Map<String,JSONObject>> getSequenceNodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        LOGGER.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getSequenceChildConfig(listServer, nodePath);
        return listServer;
    }

    //Sequence Child Config
    private static List<Map<String,JSONObject>> getSequenceChildConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {

        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                    String childPath1 = childPath + "/" + leaf;
                    listServer=getConfigData(listServer, childPath1);
                }else if(leaf.endsWith("mapping")){
                    String childPath1 = childPath + "/" + leaf;
                    listServer=getConfigData(listServer, childPath1);
                }else  {
                    String childPath1 = childPath + "/" + leaf;
                    getSequenceChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }



    //Server Config
    public static List<Map<String,JSONObject>> getServerNodeConfig(String configKey)throws Exception {
        String nodePath = CLU_PARENT_PATH + "/" + configKey;
        LOGGER.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getServerChildConfig(listServer, nodePath);
        return listServer;
    }

    //Server Child Config
    private static List<Map<String,JSONObject>> getServerChildConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {

        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        if (nodeSize==0 ){
            //处理叶子节点数据
            listServer=getConfigData(listServer, childPath);
        }else  {
            for (int i = 0; i < nodeSize; i++) {
                String leaf = iterator.next();
                if (leaf.endsWith("config")){
                    listServer=getConfigData(listServer, childPath);
                }else {
                    String childPath1 = childPath + "/" + leaf;
                    getServerChildConfig(listServer, childPath1);
                }
            }
        }
        return listServer;
    }

    //DataHost Config
    public static List<Map<String,JSONObject>> getDataHostNodeConfig(String parent_path,String configKey)throws Exception {
        String nodePath = parent_path + "/" + configKey;
        LOGGER.trace("child path is {}", nodePath);
        List<Map<String,JSONObject>> listServer = new ArrayList<>();
        listServer = getDataHostChildConfig(listServer, nodePath);
        return listServer;
    }

    //DataHost Child Config
    private static List<Map<String,JSONObject>> getDataHostChildConfig(List<Map<String,JSONObject>> listServer,String childPath) throws Exception {
        List list = new ArrayList<>();
        list = framework.getChildren().forPath(childPath);
        Iterator<String> iterator = list.iterator();
        int nodeSize = list.size();
        for (int i = 0; i < nodeSize; i++) {
            String leaf = iterator.next();
            String childPath1 = childPath + "/" + leaf;
            listServer=getConfigData(listServer, childPath1);
            getDataHostChildConfig(listServer, childPath1);
        }
        return listServer;
    }




    private static List<Map<String,JSONObject>> getConfigData(List<Map<String,JSONObject>> list,String childPath) throws IOException {


        String data= null;
        try {
            data = new String(framework.getData().forPath(childPath),"utf8");
            if (data.startsWith("[")&&data.endsWith("]")){ //JsonArray
                JSONArray jsonArray = JSONArray.parseArray(data);
//                System.out.println("----------------------JSONARRAY------------------------");
//                System.out.println("---------------------"+childPath+"-------------------------");
//                System.out.println(jsonArray);
                for (int i=0;i<jsonArray.size();i++){
                    Map<String,JSONObject> map = new HashMap<>();
                    map.put(childPath,(JSONObject)jsonArray.get(i));
                    list.add(map);
                }
                return list;
            }else {  //JsonObject

                JSONObject jsonObject = JSONObject.parseObject(data);
//                System.out.println("----------------------jsonObject------------------------");
//                System.out.println("---------------------" + childPath + "-------------------------");
//                System.out.println(jsonObject);
                Map<String,JSONObject> map = new HashMap<>();
                map.put(childPath,jsonObject);
                list.add(map);
                return list;
                //write json to xml
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //Server.xml
    public static void processServerDocument(List<Map<String,JSONObject>> mapList){
        /** 建立document对象 */
        Document document = DocumentHelper.createDocument();
        document.addDocType("mycat:server","","server.dtd");
        /** 建立serverElement根节点 */
        Element serverElement = document.addElement(QName.get("mycat:server ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            if (mapList.get(i).keySet().toString().contains("system")){
                String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
                JSONObject jsonObject = mapList.get(i).get(key);
                Element system = serverElement.addElement("system");
                if (jsonObject.containsKey("defaultsqlparser")){
                    Element defaultsqlparser = system.addElement("property").addAttribute("name", "defaultsqlparser");
                    defaultsqlparser.setText(jsonObject.getString("defaultsqlparser"));
                }
                if (jsonObject.containsKey("serverport")){
                    Element serverport = system.addElement("property").addAttribute("name", "serverport");
                    serverport.setText(jsonObject.getString("serverport"));
                }
                if (jsonObject.containsKey("sequncehandlertype")){
                    Element sequncehandlertype = system.addElement("property").addAttribute("name", "sequncehandlertype");
                    sequncehandlertype.setText(jsonObject.getString("sequncehandlertype"));
                }
                if (jsonObject.containsKey("managerPort")){
                    Element managerPort = system.addElement("property").addAttribute("name", "managerPort");
                    managerPort.setText(jsonObject.getString("managerPort"));
                }
                if (jsonObject.containsKey("charset")){
                    Element charset = system.addElement("property").addAttribute("name", "charset");
                    charset.setText(jsonObject.getString("charset"));
                }
                if (jsonObject.containsKey("registryAddress")){
                    Element registryAddress = system.addElement("property").addAttribute("name", "registryAddress");
                    registryAddress.setText(jsonObject.getString("registryAddress"));
                }
                if (jsonObject.containsKey("useCompression")){
                    Element useCompression = system.addElement("property").addAttribute("name", "useCompression");
                    useCompression.setText(jsonObject.getString("useCompression"));
                }
                if (jsonObject.containsKey("processorBufferChunk")){
                    Element processorBufferChunk = system.addElement("property").addAttribute("name", "processorBufferChunk");
                    processorBufferChunk.setText(jsonObject.getString("processorBufferChunk"));
                }
                if (jsonObject.containsKey("processors")){
                    Element processors = system.addElement("property").addAttribute("name", "processors");
                    processors.setText(jsonObject.getString("processors"));
                }
                if (jsonObject.containsKey("processorExecutor")){
                    Element processorExecutor = system.addElement("property").addAttribute("name", "processorExecutor");
                    processorExecutor.setText(jsonObject.getString("processorExecutor"));
                }
                if (jsonObject.containsKey("maxStringLiteralLength")){
                    Element maxStringLiteralLength = system.addElement("property").addAttribute("name", "maxStringLiteralLength");
                    maxStringLiteralLength.setText(jsonObject.getString("maxStringLiteralLength"));
                }
                if (jsonObject.containsKey("sequnceHandlerType")){
                    Element sequnceHandlerType = system.addElement("property").addAttribute("name", "sequnceHandlerType");
                    sequnceHandlerType.setText(jsonObject.getString("sequnceHandlerType"));
                }
                if (jsonObject.containsKey("backSocketNoDelay")){
                    Element backSocketNoDelay = system.addElement("property").addAttribute("name", "backSocketNoDelay");
                    backSocketNoDelay.setText(jsonObject.getString("backSocketNoDelay"));
                }
                if (jsonObject.containsKey("frontSocketNoDelay")){
                    Element frontSocketNoDelay = system.addElement("property").addAttribute("name", "frontSocketNoDelay");
                    frontSocketNoDelay.setText(jsonObject.getString("frontSocketNoDelay"));
                }
                if (jsonObject.containsKey("processorExecutor")){
                    Element processorExecutor = system.addElement("property").addAttribute("name", "processorExecutor");
                    processorExecutor.setText(jsonObject.getString("processorExecutor"));
                }
                if (jsonObject.containsKey("mutiNodeLimitType")){
                    Element mutiNodeLimitType = system.addElement("property").addAttribute("name", "mutiNodeLimitType");
                    mutiNodeLimitType.setText(jsonObject.getString("mutiNodeLimitType"));
                }
                if (jsonObject.containsKey("mutiNodePatchSize")){
                    Element mutiNodePatchSize = system.addElement("property").addAttribute("name", "mutiNodePatchSize");
                    mutiNodePatchSize.setText(jsonObject.getString("mutiNodePatchSize"));
                }
                if (jsonObject.containsKey("idleTimeout")){
                    Element idleTimeout = system.addElement("property").addAttribute("name", "idleTimeout");
                    idleTimeout.setText(jsonObject.getString("idleTimeout"));
                }
                if (jsonObject.containsKey("bindIp")){
                    Element bindIp = system.addElement("property").addAttribute("name", "bindIp");
                    bindIp.setText(jsonObject.getString("bindIp"));
                }
                if (jsonObject.containsKey("frontWriteQueueSize")){
                    Element frontWriteQueueSize = system.addElement("property").addAttribute("name", "frontWriteQueueSize");
                    frontWriteQueueSize.setText(jsonObject.getString("frontWriteQueueSize"));
                }
            }else if (mapList.get(i).keySet().toString().contains("user")){
                String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
                JSONObject jsonObject = mapList.get(i).get(key);
                Element user = serverElement.addElement("user").addAttribute("name", jsonObject.get("name").toString());
                if (jsonObject.containsKey("password")) {
                    Element propertyUserEl = user.addElement("property").addAttribute("name", "password");
                    propertyUserEl.setText(jsonObject.get("password").toString());
                }
                if (jsonObject.containsKey("schemas")) {
                    Element propertyUserEl1 = user.addElement("property").addAttribute("name", "schemas");
                    propertyUserEl1.setText(jsonObject.get("schemas").toString().replace("[\"","").replace("\"]",""));
                }
                if (jsonObject.containsKey("readOnly")) {
                    Element propertyUserEl1 = user.addElement("property").addAttribute("name", "readOnly");
                    propertyUserEl1.setText(jsonObject.get("readOnly").toString());
                }
            }
        }
        json2XmlFile(document,"server.xml");
    }

    //rule.xml
    public static void processRuleDocument(List<Map<String,JSONObject>> mapList){
        /** 建立document对象 */
        Document document = DocumentHelper.createDocument();
        /** 建立serverElement根节点 */
        document.addDocType("mycat:rule","","rule.dtd");
        Element serverElement = document.addElement(QName.get("mycat:rule ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            String key = mapList.get(i).keySet().toString().replace("[","").replace("]","").trim();
            JSONObject jsonObject = mapList.get(i).get(key);
                    //tableRule
            Element ruleEl = serverElement.addElement("tableRule").addAttribute("name",jsonObject.getString("name"));
            if (jsonObject.containsKey("column")){
                Element ruleEl1 = ruleEl.addElement("rule");
                Element columns = ruleEl1.addElement("columns");
                columns.setText(jsonObject.getString("column"));

                if (jsonObject.containsKey("name")) {
                    Element algorithm = ruleEl1.addElement("algorithm");
                    algorithm.setText(jsonObject.getString("name"));
                }
            }
        }

        //function
        for (int i=0;i<mapList.size();i++) {
            String key = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            JSONObject jsonObject = mapList.get(i).get(key);
            Element system = serverElement.addElement("function");
            if (jsonObject.containsKey("name")) {
                system.addAttribute("name", jsonObject.getString("name"));
            }
            if (jsonObject.containsKey("functionName")) {
                //1.4 class
                String pathFor14 = "org.opencloudb.route.function";
                String func = jsonObject.getString("functionName");
                String className = pathFor14 + func.substring(func.lastIndexOf("."), func.length());
                system.addAttribute("class", className);
            }
            if (jsonObject.containsKey("count")) {
                Element serverport = system.addElement("property").addAttribute("name", "count");
                serverport.setText(jsonObject.getString("count"));
            }
            if (jsonObject.containsKey("virtualBucketTimes")) {
                Element serverport = system.addElement("property").addAttribute("name", "virtualBucketTimes");
                serverport.setText(jsonObject.getString("virtualBucketTimes"));
            }
            if (jsonObject.containsKey("partitionCount")) {
                Element serverport = system.addElement("property").addAttribute("name", "partitionCount");
                serverport.setText(jsonObject.getString("partitionCount"));
            }
            if (jsonObject.containsKey("partitionLength")) {
                Element serverport = system.addElement("property").addAttribute("name", "partitionLength");
                serverport.setText(jsonObject.getString("partitionLength"));
            }
            if (jsonObject.containsKey("splitOneDay")) {
                Element serverport = system.addElement("property").addAttribute("name", "splitOneDay");
                serverport.setText(jsonObject.getString("splitOneDay"));
            }
            if (jsonObject.containsKey("dateFormat")) {
                Element serverport = system.addElement("property").addAttribute("name", "dateFormat");
                serverport.setText(jsonObject.getString("dateFormat"));
            }
            if (jsonObject.containsKey("sBeginDate")) {
                Element serverport = system.addElement("property").addAttribute("name", "sBeginDate");
                serverport.setText(jsonObject.getString("sBeginDate"));
            }
            if (jsonObject.containsKey("type")) {
                Element serverport = system.addElement("property").addAttribute("name", "type");
                serverport.setText(jsonObject.getString("type"));
            }
            if (jsonObject.containsKey("totalBuckets")) {
                Element serverport = system.addElement("property").addAttribute("name", "totalBuckets");
                serverport.setText(jsonObject.getString("totalBuckets"));
            }

            //mapFile from config
            if (jsonObject.containsKey("config")) {
                String config = jsonObject.getString("config").replace("{", "").replace("}", "").replace("\"", "").replace(":", "=");
                String mapFile = jsonObject.getString("name");
                Element mapFileEl = system.addElement("property").addAttribute("name", "mapFile");
                mapFileEl.setText(mapFile + ".txt");
                conf2File("/" + mapFile + ".txt", config);
            }
            if (jsonObject.containsKey("groupPartionSize")) {
                Element serverport = system.addElement("property").addAttribute("name", "groupPartionSize");
                serverport.setText(jsonObject.getString("groupPartionSize"));
            }
            if (jsonObject.containsKey("sPartionDay")) {
                Element serverport = system.addElement("property").addAttribute("name", "sPartionDay");
                serverport.setText(jsonObject.getString("sPartionDay"));
            }
        }
        json2XmlFile(document,"rule.xml");
    }


    //Schema.xml
    public static void processSchemaDocument(List<Map<String,JSONObject>> mapList) throws Exception {

        Document document = DocumentHelper.createDocument();
        document.addDocType("mycat:schema","","schema.dtd");
        /** 建立serverElement根节点 */
        Element serverElement = document.addElement(QName.get("mycat:schema ", "http://org.opencloudb/"));
        for (int i=0;i<mapList.size();i++){
            int subLength = CLU_PARENT_PATH.length()+SCHEMA_CONFIG_DIRECTORY.length()+2;
            String SchemaPath = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            if (!SchemaPath.substring(subLength,SchemaPath.length()).contains("/")){
                String schema =SchemaPath.substring(subLength,SchemaPath.length());
                JSONObject jsonObject = mapList.get(i).get(SchemaPath);
                Element schemaEl = serverElement.addElement("schema");
                if (!schema.isEmpty()){
                    Element schemaElCon = schemaEl.addAttribute("name", schema);
                    if (jsonObject.containsKey("checkSQLSchema"))
                        schemaElCon.addAttribute("checkSQLschema",jsonObject.get("checkSQLSchema").toString());
                    if (jsonObject.containsKey("defaultMaxLimit"))
                        schemaElCon.addAttribute("sqlMaxLimit",jsonObject.get("defaultMaxLimit").toString());
                    if (jsonObject.containsKey("dataNode"))
                        schemaElCon.addAttribute("dataNode",jsonObject.get("dataNode").toString());
                    //处理 table
                    for (int j=0;j<mapList.size();j++) {
                        String tablePath = mapList.get(j).keySet().toString().replace("[", "").replace("]", "").trim();
                        if (!tablePath.contains(schema)){
                            continue;
                        }
                        String temp = tablePath.substring(subLength, tablePath.length());
                        if (temp.contains("/") && temp.contains(schema)&&temp.lastIndexOf("/")<=schema.length()) {
                            String tableName = temp.substring(schema.length() + 1, temp.length());
//                            System.out.println("table:" + tableName);
                            JSONObject tableJsonObject = mapList.get(j).get(tablePath);
                            Element tableEl = schemaEl.addElement("table");
                            if (tableJsonObject.containsKey("name"))
                                tableEl.addAttribute("name", tableJsonObject.get("name").toString());
                            if (tableJsonObject.containsKey("primaryschema"))
                                tableEl.addAttribute("primaryschema", tableJsonObject.get("primaryschema").toString());
                            if (tableJsonObject.containsKey("datanode"))
                                tableEl.addAttribute("dataNode", tableJsonObject.get("datanode").toString());
                            if (tableJsonObject.containsKey("type"))
                                if (tableJsonObject.get("type").toString().startsWith("1")) //目前只有1 全局表
                                tableEl.addAttribute("type", "global");
                            if (tableJsonObject.containsKey("ruleName"))
                                tableEl.addAttribute("rule", tableJsonObject.get("ruleName").toString());
                            //处理childTable
                            for (int k=0;k<mapList.size();k++) {
                                String childTablePath = mapList.get(k).keySet().toString().replace("[", "").replace("]", "").trim();
                                if (!childTablePath.contains(schema)){
                                    continue;
                                }
                                if (childTablePath.equals(tablePath)||childTablePath.compareTo(tablePath)<=0)
                                    continue;
                                String tempChildTableName = childTablePath.substring(tablePath.length()-tableName.length(), childTablePath.length());
                                if (tempChildTableName.contains("/") && tempChildTableName.contains(tableName)) {
                                    String childTable = tempChildTableName.substring(tableName.length() + 1, tempChildTableName.length());
                                    if (tempChildTableName.substring(tempChildTableName.lastIndexOf("/")+1,tempChildTableName.length()).equals(childTable)){
//                                        System.out.println("childTable:" + childTable);
                                        JSONObject childTableJsonObject = mapList.get(k).get(childTablePath);
                                        Element childTableEl = tableEl.addElement("childTable");
                                        if (childTableJsonObject.containsKey("name"))
                                            childTableEl.addAttribute("name", childTableJsonObject.get("name").toString());
                                        if (childTableJsonObject.containsKey("primarykey"))
                                            childTableEl.addAttribute("primaryKey", childTableJsonObject.get("primarykey").toString());
                                        if (childTableJsonObject.containsKey("parentkey"))
                                            childTableEl.addAttribute("parentKey", childTableJsonObject.get("parentkey").toString());
                                        if (childTableJsonObject.containsKey("joinkey"))
                                            childTableEl.addAttribute("joinKey", childTableJsonObject.get("joinkey").toString());
                                        //处理 child-childTable
                                        for (int l=0;l<mapList.size();l++) {
                                            String child_childTablePath = mapList.get(l).keySet().toString().replace("[", "").replace("]", "").trim();
                                            if (!child_childTablePath.contains(schema)){
                                                continue;
                                            }
                                            if (child_childTablePath.equals(childTablePath)||child_childTablePath.compareTo(childTablePath)<=0||!child_childTablePath.contains(childTable))
                                                continue;
                                            String tempchild_childTablePath = child_childTablePath.substring(subLength+schema.length()+tableName.length()+2, child_childTablePath.length());
                                            if (tempchild_childTablePath.contains("/") && tempchild_childTablePath.contains(childTable)) {
                                                String child_childTablePathName = tempchild_childTablePath.substring(childTable.length() + 1, tempchild_childTablePath.length());
                                                if (tempchild_childTablePath.substring(tempchild_childTablePath.lastIndexOf("/")+1,tempchild_childTablePath.length()).equals(child_childTablePathName)){
                                                    //System.out.println("child-childTable:" + child_childTablePathName);
                                                    JSONObject child_childTableJsonObject = mapList.get(l).get(child_childTablePath);
                                                    Element child_childTablePathEl = childTableEl.addElement("childTable");
                                                    if (child_childTableJsonObject.containsKey("name"))
                                                        child_childTablePathEl.addAttribute("name", child_childTableJsonObject.get("name").toString());
                                                    if (child_childTableJsonObject.containsKey("primarykey"))
                                                        child_childTablePathEl.addAttribute("primaryKey", child_childTableJsonObject.get("primarykey").toString());
                                                    if (child_childTableJsonObject.containsKey("parentkey"))
                                                        child_childTablePathEl.addAttribute("parentKey", child_childTableJsonObject.get("parentkey").toString());
                                                    if (child_childTableJsonObject.containsKey("joinkey"))
                                                        child_childTablePathEl.addAttribute("joinKey", child_childTableJsonObject.get("joinkey").toString());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }}}
        }
        //datanode
        List<Map<String,JSONObject>> listDataNode = getDatanodeConfig(DATANODE_CONFIG_DIRECTORY);
        processDataNodeDocument(serverElement,listDataNode);

        //datahost
        List<Map<String,JSONObject>> listDataHost = getDataHostNodeConfig(CLU_PARENT_PATH,DATAHOST_CONFIG_DIRECTORY);
        processMysqlRepDocument(serverElement, listDataHost);
        json2XmlFile(document,"schema.xml");
    }


    //Datahost.xml
    public static void processDatahostDocument(Element serverElement,List<Map<String,JSONObject>> mapList){
        for (int i=0;i<mapList.size();i++){
        int subLength = CLU_PARENT_PATH.length()+DATAHOST_CONFIG_DIRECTORY.length()+2;
        String datahostName = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
        if (!datahostName.substring(subLength,datahostName.length()).contains("/")){
            String key =datahostName.substring(subLength,datahostName.length());
            JSONObject jsonObject = mapList.get(i).get(datahostName);
            Element dataHost = serverElement.addElement("dataHost");
            if (!key.isEmpty()){
                Element datahost = dataHost.addAttribute("name", key);
                if (jsonObject.containsKey("writetype"))
                datahost.addAttribute("writeType",jsonObject.get("writetype").toString());
                if (jsonObject.containsKey("switchType"))
                    datahost.addAttribute("switchType",jsonObject.get("switchType").toString());
                if (jsonObject.containsKey("slaveThreshold"))
                    datahost.addAttribute("slaveThreshold",jsonObject.get("slaveThreshold").toString());
                if (jsonObject.containsKey("balance"))
                    datahost.addAttribute("balance",jsonObject.get("balance").toString());
                if (jsonObject.containsKey("dbtype"))
                    datahost.addAttribute("dbType",jsonObject.get("dbtype").toString());
                if (jsonObject.containsKey("maxcon"))
                    datahost.addAttribute("maxCon",jsonObject.get("maxcon").toString());
                if (jsonObject.containsKey("mincon"))
                    datahost.addAttribute("minCon",jsonObject.get("mincon").toString());
                if (jsonObject.containsKey("dbDriver"))
                    datahost.addAttribute("dbDriver",jsonObject.get("dbDriver").toString());
                if (jsonObject.containsKey("heartbeatSQL")){
                    Element  heartbeatSQL = dataHost.addElement("heartbeat");
                    heartbeatSQL.setText(jsonObject.get("heartbeatSQL").toString());
                }
                //处理WriteHost
                for (int j=0;j<mapList.size();j++) {
                    String host = mapList.get(j).keySet().toString().replace("[", "").replace("]", "").trim();
                    String temp = host.substring(subLength, host.length());
                    if (temp.contains("/") && temp.contains(key)&&temp.lastIndexOf("/")<=key.length()) {
                        String childHost = temp.substring(key.length() + 1, temp.length());
                        //System.out.println("childHost:" + childHost);
                        JSONObject childJsonObject = mapList.get(j).get(host);
                        Element writeHost = dataHost.addElement("writeHost");
                        if (childJsonObject.containsKey("host"))
                            writeHost.addAttribute("host", childJsonObject.get("host").toString());
                        if (childJsonObject.containsKey("url"))
                            writeHost.addAttribute("url", childJsonObject.get("url").toString());
                        if (childJsonObject.containsKey("user"))
                            writeHost.addAttribute("user", childJsonObject.get("user").toString());
                        if (childJsonObject.containsKey("password"))
                            writeHost.addAttribute("password", childJsonObject.get("password").toString());
                        //处理readHost
                        for (int k=0;k<mapList.size();k++) {
                            String readhost = mapList.get(k).keySet().toString().replace("[", "").replace("]", "").trim();
                            if (readhost.equals(host)||readhost.compareTo(host)<=0)
                                continue;
                            String tempread = readhost.substring(host.length()-childHost.length(), readhost.length());
                            if (tempread.contains("/") && tempread.contains(childHost)) {
                                String readHost = tempread.substring(childHost.length() + 1, tempread.length());
                                //System.out.println("readHost:" + readHost);
                                JSONObject readJsonObject = mapList.get(k).get(readhost);
                                Element readHostEl = writeHost.addElement("readHost");
                                if (readJsonObject.containsKey("host"))
                                    readHostEl.addAttribute("host", readJsonObject.get("host").toString());
                                if (readJsonObject.containsKey("url"))
                                    readHostEl.addAttribute("url", readJsonObject.get("url").toString());
                                if (readJsonObject.containsKey("user"))
                                    readHostEl.addAttribute("user", readJsonObject.get("user").toString());
                                if (readJsonObject.containsKey("password"))
                                    readHostEl.addAttribute("password", readJsonObject.get("password").toString());
                            }
                        }
                    }
                }
            }
        }
        }
       //json2XmlFile(document,"D:/dataHost.xml");
    }

    //Datanode xml
    public static void processDataNodeDocument(Element serverElement,List<Map<String,JSONObject>> mapList){
        for (int i=0;i<mapList.size();i++){
            String dataNode = mapList.get(i).keySet().toString().replace("[", "").replace("]", "").trim();
            JSONObject jsonObject = mapList.get(i).get(dataNode);
            Element dataNodeEl = serverElement.addElement("dataNode");
            if (jsonObject.containsKey("name"))
                dataNodeEl.addAttribute("name", jsonObject.get("name").toString());
            if (jsonObject.containsKey("dataHost"))
                dataNodeEl.addAttribute("dataHost", jsonObject.get("dataHost").toString());
            if (jsonObject.containsKey("database"))
                dataNodeEl.addAttribute("database", jsonObject.get("database").toString());
            }
        //json2XmlFile(document,"D:/dataNode.xml");
    }



    public static boolean json2XmlFile(Document document,String filename) {
        boolean flag = true;
        try
        {
            /* 将document中的内容写入文件中 */
            String filePath = SystemConfig.getHomePath()+"/src/main/resources/";
            filename = filePath+filename;
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setEncoding("UTF8");
            XMLWriter writer = new XMLWriter(new FileWriter(new File(filename)),format);
            writer.write(document);
            writer.close();
        }catch(Exception ex)
        {
            flag = false;
            ex.printStackTrace();
        }
        return flag;
    }



    @SuppressWarnings("unchecked")
	private static Map<String, Object> loadZkConfig() {
        InputStream configIS = ZkDownload.class.getResourceAsStream(ZK_CONFIG_FILE_NAME);
        if (configIS == null) {
            throw new RuntimeException("can't find zk properties file : " + ZK_CONFIG_FILE_NAME);
        }
        return (Map<String, Object>) new Yaml().load(configIS);
    }

    private static CuratorFramework createConnection(String url) {
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .newClient(url, new ExponentialBackoffRetry(100, 6));

        //start connection
        curatorFramework.start();
        //wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException e) {
        }

        //fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }


}
