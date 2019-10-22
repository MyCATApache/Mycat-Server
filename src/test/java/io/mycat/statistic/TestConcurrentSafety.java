package io.mycat.statistic;

import io.mycat.server.parser.ServerParse;
import io.mycat.statistic.stat.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试SQLstat相关元素并发安全性
 *
 *
 *
 *
 *
 *
 * 此单元测试会造成服务器上build运行时间过长一直通不过，最多build了6天还没结束，所以先忽略
 *
 *
 *
 *  后续修复好了再打开
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 08:54 2016/5/16
 */
public class TestConcurrentSafety {
    private static final int THREAD_COUNT = 2;
    private static final int LOOP_COUNT = 1000;

    String sql = "SELECT `fnum`, `forg`, `fdst`, `airline`, `ftype` , `ports_of_call`, " +
            "`scheduled_deptime`, `scheduled_arrtime`, `actual_deptime`, `actual_arrtime`, " +
            "`flight_status_code` FROM dynamic " +
            "WHERE `fnum` = 'CA1'  AND `forg` = 'PEK'  AND `fdst` = 'SHA' " +
            "AND `scheduled_deptime` BETWEEN 1212121 AND 232323233 " +
            "AND `fservice` = 'J' AND `fcategory` = 1 " +
            "AND `share_execute_flag` = 1 ORDER BY scheduled_deptime";

    String sql2 = "SELECT `fnum`, `forg`, `fdst`, `airline`, `ftype` , `ports_of_call`, " +
            "`scheduled_deptime`, `scheduled_arrtime`, `actual_deptime`, `actual_arrtime`, " +
            "`flight_status_code` FROM dynamic " +
            "WHERE `fnum` = 'CA2'  AND `forg` = 'PEK'  AND `fdst` = 'SHA' " +
            "AND `scheduled_deptime` BETWEEN 1212121 AND 232323233 " +
            "AND `fservice` = 'J' AND `fcategory` = 1 " +
            "AND `share_execute_flag` = 1 ORDER BY scheduled_deptime";

    String sql3 = "SELECT `fnum`, `forg`, `fdst`, `airline`, `ftype` , `ports_of_call`, " +
            "`scheduled_deptime`, `scheduled_arrtime`, `actual_deptime`, `actual_arrtime`, " +
            "`flight_status_code` FROM dynamic " +
            "WHERE `fnum` = 'CA3'  AND `forg` = 'PEK'  AND `fdst` = 'SHA' " +
            "AND `scheduled_deptime` BETWEEN 1212121 AND 232323233 " +
            "AND `fservice` = 'J' AND `fcategory` = 1 " +
            "AND `share_execute_flag` = 1 ORDER BY scheduled_deptime";

    String sql4 = "SELECT `fnum`, `forg`, `fdst`, `airline`, `ftype` , `ports_of_call`, " +
            "`scheduled_deptime`, `scheduled_arrtime`, `actual_deptime`, `actual_arrtime`, " +
            "`flight_status_code` FROM dynamic " +
            "WHERE `fnum` = 'CA3'  AND `forg` = 'PEK'";


    @Test  @Ignore
    public void testQueryConditionAnalyzer() throws InterruptedException {


        final QueryResult qr = new QueryResult("zhuam", ServerParse.SELECT, sql, 0, 0, 0, 0, 0,0,       "127.0.0.1");
        final QueryResult qr2 = new QueryResult("zhuam", ServerParse.SELECT, sql2, 0, 0, 0, 0, 0,0,       "127.0.0.1");
        final QueryResult qr3 = new QueryResult("zhuam", ServerParse.SELECT, sql3, 0, 0, 0, 0, 0,0,       "127.0.0.1");

        final QueryConditionAnalyzer analyzer = QueryConditionAnalyzer.getInstance();
        analyzer.setCf("dynamic&fnum");

        Thread thread[] = new Thread[THREAD_COUNT];
        Thread thread2[] = new Thread[THREAD_COUNT];
        Thread thread3[] = new Thread[THREAD_COUNT];

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        analyzer.onQueryResult(qr);
                    }
                }
            };

            thread2[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        analyzer.onQueryResult(qr2);
                    }
                }
            };

            thread3[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        analyzer.onQueryResult(qr3);
                    }
                }
            };
        }

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i].start();
            thread2[i].start();
            thread3[i].start();
        }

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i].join();
            thread2[i].join();
            thread3[i].join();
        }

        List<Map.Entry<Object, AtomicLong>> list = analyzer.getValues();
        Assert.assertTrue((list.get(0).getValue().get() == (long) THREAD_COUNT * LOOP_COUNT));
        Assert.assertTrue((list.get(1).getValue().get() == (long) THREAD_COUNT * LOOP_COUNT));
        Assert.assertTrue((list.get(2).getValue().get() == (long) THREAD_COUNT * LOOP_COUNT));
    }

    @Test       @Ignore
    public void testUserSqlHighStat() throws InterruptedException {
        final UserSqlHighStat userSqlHighStat = new UserSqlHighStat();

        Thread thread[] = new Thread[THREAD_COUNT];
        Thread thread2[] = new Thread[THREAD_COUNT];
        Thread thread3[] = new Thread[THREAD_COUNT];

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        userSqlHighStat.addSql(sql, 10L, 1L, 11L,      "127.0.0.1");
                    }
                }
            };

            thread2[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        userSqlHighStat.addSql(sql2, 10L, 1L, 11L,      "127.0.0.1");
                    }
                }
            };

            thread3[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < LOOP_COUNT; j++) {
                        userSqlHighStat.addSql(sql4, 10L, 1L, 11L,      "127.0.0.1");
                    }
                }
            };
        }

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i].start();
            thread2[i].start();
            thread3[i].start();
        }

        for (int i = 0; i < THREAD_COUNT; i++) {
            thread[i].join();
            thread2[i].join();
            thread3[i].join();
        }

        List<SqlFrequency> sqlFrequency = userSqlHighStat.getSqlFrequency(true);
        Assert.assertTrue(sqlFrequency.size() == 2);
        Assert.assertTrue(sqlFrequency.get(0).getCount() == 2 * THREAD_COUNT *LOOP_COUNT);
        Assert.assertTrue(sqlFrequency.get(1).getCount() == THREAD_COUNT *LOOP_COUNT);
    }



}
