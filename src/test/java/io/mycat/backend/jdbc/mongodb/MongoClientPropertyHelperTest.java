package io.mycat.backend.jdbc.mongodb;

import org.junit.Test;

import java.util.Properties;

/**
 * @author liuxinsi
 * @mail akalxs@gmail.com
 */
public class MongoClientPropertyHelperTest {
    @Test
    public void testFormatProperties() {
        Properties pro = new Properties();
        pro.put("authMechanism", "SCRAM-SHA-1");
        pro.put("readPreference", "nearest");
        pro.put("maxPoolSize", 10);
        pro.put("ssl", true);
        String options = MongoClientPropertyHelper.formatProperties(pro);
        System.out.println(options);

    }
}
