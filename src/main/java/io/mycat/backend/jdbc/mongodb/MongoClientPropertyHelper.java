package io.mycat.backend.jdbc.mongodb;


import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author liuxinsi
 * @mail akalxs@gmail.com
 */
public class MongoClientPropertyHelper {
    /**
     * 格式化<code>pro</code>中的属性为{@link com.mongodb.MongoClientURI}中要求的格式。
     *
     * @param pro 配置参数
     * @return 格式化后的字符串
     */
    public static String formatProperties(Properties pro) {
        if (pro == null || pro.isEmpty()) {
            return null;
        }

        Set<Object> keys = pro.keySet();
        List<String> props = new ArrayList<>(keys.size());
        for (Object key : keys) {
            Object value = pro.get(key);
            props.add(key + "=" + value.toString());
        }
        return Joiner.on(";").join(props);
    }
}
