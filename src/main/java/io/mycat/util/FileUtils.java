package io.mycat.util;

import java.io.File;

import io.mycat.config.model.SystemConfig;

/**
 * ${DESCRIPTION}
 *
 * @author dengliaoyan
 * @create 2018/7/27
 */
public class FileUtils {

    public static void mkDirectory(String directoryAbsolutePath){
        File directory = new File(directoryAbsolutePath);
        if(!directory.exists()){
            directory.mkdirs();
        }
        directory = null;
    }
}
