package io.mycat.route.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Property文件加载器
 *
 * @author Hash Zhang
 * @time 00:08:03 2016/5/3
 * @version 1.0
 */
public class PropertiesUtil {
    public static Properties loadProps(String propsFile){
        Properties props = new Properties();
        InputStream inp = Thread.currentThread().getContextClassLoader().getResourceAsStream(propsFile);

        if (inp == null) {
            throw new java.lang.RuntimeException("time sequnce properties not found " + propsFile);
        }
        try {
            props.load(inp);
        } catch (IOException e) {
            throw new java.lang.RuntimeException(e);
        }
        return props;
    }
}
