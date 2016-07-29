package io.mycat.route.function;

import com.google.common.hash.Hashing;

import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteStrategy;
import io.mycat.route.factory.RouteStrategyFactory;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLNonTransientException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class PartitionByRangeDateHashTest
{

    @Test
    public void test() throws ParseException {
        PartitionByRangeDateHash partition = new PartitionByRangeDateHash();

        partition.setDateFormat("yyyy-MM-dd HH:mm:ss");
        partition.setsBeginDate("2014-01-01 00:00:00");
        partition.setsPartionDay("3");
        partition.setGroupPartionSize("6");

        partition.init();

        Integer calculate = partition.calculate("2014-01-01 00:00:00");
        Assert.assertEquals(true, 3 == calculate);

         calculate = partition.calculate("2014-01-01 00:00:01");
        Assert.assertEquals(true, 1 == calculate);

        calculate = partition.calculate("2014-01-04 00:00:00");
        Assert.assertEquals(true, 7 == calculate);

        calculate = partition.calculate("2014-01-04 00:00:01");
        Assert.assertEquals(true, 11== calculate);


        Date beginDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2014-01-01 00:00:00");
        Calendar cal = Calendar.getInstance();
        cal.setTime(beginDate);


        for (int i = 0; i < 60*60*24*3-1; i++)
        {
              cal.add(Calendar.SECOND, 1);
        int v=    partition.calculate(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime()))     ;
            Assert.assertTrue(v<6);
        }


    }



    protected Map<String, SchemaConfig> schemaMap;
    protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");

    public PartitionByRangeDateHashTest() {
        String schemaFile = "/route/schema.xml";
        String ruleFile = "/route/rule.xml";
        SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
        schemaMap = schemaLoader.getSchemas();
    }

    @Test
    public void testRange() throws SQLNonTransientException {
        String sql = "select * from offer1  where col_date between '2014-01-01 00:00:00'  and '2014-01-03 23:59:59'     order by id desc limit 100";
        SchemaConfig schema = schemaMap.get("TESTDB");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        junit.framework.Assert.assertEquals(6, rrs.getNodes().length);

        sql = "select * from offer1  where col_date between '2014-01-01 00:00:00'  and '2014-01-04 00:00:59'      order by id desc limit 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        junit.framework.Assert.assertEquals(12, rrs.getNodes().length);

        sql = "select * from offer1  where col_date between '2014-01-04 00:00:00'  and '2014-01-06 23:59:59'      order by id desc limit 100";
        rrs = routeStrategy.route(new SystemConfig(), schema, -1, sql, null,
                null, cachePool);
        junit.framework.Assert.assertEquals(6, rrs.getNodes().length);


    }

     public static int hash(long str,int size)
     {
     return     Hashing.consistentHash(str,size)      ;
     }

    public static void main(String[] args) throws ParseException
    {

        Map map=new HashMap<>()  ;
        Date beginDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2014-01-04 00:00:01");
        for (int i = 0; i < 60*60*24*10; i++)
        {


            Calendar cal = Calendar.getInstance();
            cal.setTime(beginDate);
           cal.add(Calendar.SECOND, 1);
            beginDate = cal.getTime();
            int hash = hash(beginDate.getTime(), 3);
            if(map.containsKey(hash))
            {
            map.put(hash,    (int)map.get(hash)+1);
            } else
            {
                map.put(hash,1);
            }
          //  System.out.println(hash);
        }


        System.out.println(map.values());
    }
}
