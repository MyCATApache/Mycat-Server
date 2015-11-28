package org.opencloudb.config;

import demo.catlets.ZkDownload;
import org.opencloudb.config.model.SystemConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * Created by StoneGod on 2015/11/23.
 */
public class ZkConfig {

    public static  String DEFAULT_CONF = "mycat.conf";
    //zk配置
    private  String zkUrl;
    private  String zkID;
    private String zkPort;
    private String zkClu;
    private String zkZone;
    private String zkLb;

    public String getZkZone() {
        return zkZone;
    }

    public void setZkZone(String zkZone) {
        this.zkZone = zkZone;
    }

    public String getZkLb() {
        return zkLb;
    }

    public void setZkLb(String zkLb) {
        this.zkLb = zkLb;
    }

    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkUrl() {
        return zkUrl;
    }

    public void setZkUrl(String zkUrl) {
        this.zkUrl = zkUrl;
    }

    public String getZkID() {
        return zkID;
    }

    public void setZkID(String zkID) {
        this.zkID = zkID;
    }

    public String getZkClu() {
        return zkClu;
    }

    public void setZkClu(String zkClu) {
        this.zkClu = zkClu;
    }

    //read mycat.conf for zk
    public void isUsedZk(){
        try {
            String encoding="UTF8";
            String filePath = SystemConfig.getHomePath()+"/src/main/resources/";
            File file=new File(filePath+DEFAULT_CONF);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while((lineTxt = bufferedReader.readLine()) != null){
                    if (lineTxt.startsWith("zkID")){
                        setZkID(lineTxt.substring(lineTxt.indexOf("=") + 1));
                        continue;
                    }else if (lineTxt.startsWith("zkUrl")){
                        setZkUrl(lineTxt.substring(lineTxt.indexOf("=")+1));
                        continue;
                    }else if (lineTxt.startsWith("zkPort")){
                        setZkPort(lineTxt.substring(lineTxt.indexOf("=")+1));
                        continue;
                    }else if (lineTxt.startsWith("zkClu")){
                        setZkClu(lineTxt.substring(lineTxt.indexOf("=") + 1));
                        continue;
                    }else if (lineTxt.startsWith("zkZone")){
                        setZkZone(lineTxt.substring(lineTxt.indexOf("=") + 1));
                        continue;
                    }
                    else if (lineTxt.startsWith("zkLb")){
                        setZkLb(lineTxt.substring(lineTxt.indexOf("=") + 1));
                        continue;
                    }
                }
                read.close();
            }else{
                System.out.println(DEFAULT_CONF + " file no exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initLocalXmlFromZk(ZkConfig zkConfig){
        zkConfig.isUsedZk();
        //zk start or not,be sure zkurl is same as zkcreate.yaml's zkconfig
        if (zkConfig.getZkUrl()!=null && zkConfig.getZkID()!=null&& zkConfig.getZkPort()!=null&& zkConfig.getZkClu()!=null){
            //upload data to zk,only use in init data...
//           boolean zkcreate =  ZkCreate.init(zkConfig.getZkUrl(), zkConfig.getZkPort(), zkConfig.getZkClu(), zkConfig.getZkID());
//            if (!zkcreate)
//                System.exit(1);
            //download zk data to local xml
            try {
                boolean zkdownload = ZkDownload.init(zkConfig.getZkUrl(),zkConfig.getZkPort(),zkConfig.getZkClu(), zkConfig.getZkID());
                if (!zkdownload)
                    System.exit(1);
                System.out.println("-------------------------------------------------");
                System.out.println("Wait for write xml finished...");
                //this.wait(3000L);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
