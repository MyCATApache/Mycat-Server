package io.mycat.config.loader.zkprocess.xmltozk;

import com.google.common.io.Files;
import io.mycat.config.model.SystemConfig;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;

/**
 * Created by magicdoom on 2016/10/26.
 *  only for test
 */
public class BindataToZK {
    public static void main(String[] args) {
        File file = new File(SystemConfig.getHomePath()+ "/conf","ruledata" );
        if(file.exists()&&file.isDirectory())
        {
            File[] binFiles=file.listFiles();
            for (File binFile : binFiles) {

           String path=     ZKUtils.getZKBasePath()+"ruledata/"+binFile.getName();
                CuratorFramework zk= ZKUtils.getConnection();
                try {
                    zk.create().creatingParentsIfNeeded().forPath(path)  ;
                    zk.setData().forPath(path, Files.toByteArray(binFile)) ;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }
    }
}
