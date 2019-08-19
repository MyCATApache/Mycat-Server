package io.mycat.server;

import io.mycat.server.util.SchemaUtil;
import junit.framework.Assert;
import org.junit.Test;

/**
 * @description:
 * @author: dengliaoyan
 * @create: 2019-03-13 16:41
 **/
public class SchemaUtilTest {

    @Test
    public void parseSchema() {
        String sql = "INSERT INTO travelrecord(id,user_id,traveldate,fee,days) VALUES(NEXT VALUE FOR MYCATSEQ_travelrecord,'330002',NOW(),1235,6); ";
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
        Assert.assertNotNull(schemaInfo);
        Assert.assertNotNull(schemaInfo.table);
        Assert.assertEquals("travelrecord", schemaInfo.table);
    }
}
