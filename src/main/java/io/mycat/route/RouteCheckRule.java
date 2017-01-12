package io.mycat.route;

import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *    迁移切换时准备切换阶段需要禁止写操作和读所有分片的sql
 */
public class RouteCheckRule {
    public static ConcurrentMap<String,ConcurrentMap<String,List<Range>>> migrateRuleMap=new ConcurrentHashMap<>();

}
