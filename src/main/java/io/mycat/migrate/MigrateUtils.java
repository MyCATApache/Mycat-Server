package io.mycat.migrate;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.config.model.DBHostConfig;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.util.ZKUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static io.mycat.route.function.PartitionByCRC32PreSlot.*;

/**
 * Created by magicdoom on 2016/9/16.
 */
public class MigrateUtils {

    /**
     *扩容计算，以每一个源节点到一个目标节点为一个任务
     * @param table
     * @param integerListMap  会进行修改，所以传入前请自己clone一份
     * @param oldDataNodes
     * @param newDataNodes
     * @param slotsTotalNum
     * @return
     */
    public static Map<String, List<MigrateTask>> balanceExpand(String table, Map<Integer, List<Range>> integerListMap, List<String> oldDataNodes,
            List<String> newDataNodes,int slotsTotalNum) {

        int newNodeSize = oldDataNodes.size() + newDataNodes.size();
        int newSlotPerNode =slotsTotalNum / newNodeSize;
        Map<String, List<MigrateTask>> newNodeTask = new HashMap<>();
         int gb=slotsTotalNum-newSlotPerNode*(newNodeSize);
        for (int i = 0; i < integerListMap.size(); i++) {

            List<Range> rangeList = integerListMap.get(i);
            int needMoveNum = getCurTotalSize(rangeList) - newSlotPerNode;
            List<Range> allMoveList = getPartAndRemove(rangeList, needMoveNum);
            for (int i1 = 0; i1 < newDataNodes.size(); i1++) {
                String newDataNode = newDataNodes.get(i1);
                if (allMoveList.size() == 0)
                    break;
                List<MigrateTask> curRangeList = newNodeTask.get(newDataNode);
                if (curRangeList == null)
                    curRangeList = new ArrayList<>();
                int hasSlots = getCurTotalSizeForTask(curRangeList);
                int needMove =( i1==0)?newSlotPerNode - hasSlots+gb:newSlotPerNode - hasSlots;
                if (needMove > 0) {
                    List<Range> moveList = getPartAndRemove(allMoveList, needMove);
                    MigrateTask task = new MigrateTask();
                    task.setFrom( oldDataNodes.get(i));
                    task.setTo( newDataNode);
                    task.setTable(table);
                    task.setSlots( moveList);
                    curRangeList.add(task);
                    newNodeTask.put(newDataNode, curRangeList);
                }
            }

            if(allMoveList.size()>0)
            {
                throw new RuntimeException("some slot has not moved to")  ;
            }


        }

        return newNodeTask;
    }



    private static List<Range> getPartAndRemove(List<Range> rangeList, int size) {
        List<Range> result = new ArrayList<>();

        for (int i = 0; i < rangeList.size(); i++) {

            Range range = rangeList.get(i);
            if (range == null)
                continue;
            if (range.size == size) {
                result.add(new Range(range.start, range.end));
                rangeList.set(i,null);
                break;
            } else if (range.size < size) {
                result.add(new Range(range.start, range.end));
                size = size - range.size;
                rangeList.set(i,null);
            } else if (range.size > size) {
                result.add(new Range(range.start, range.start+ size - 1));
                rangeList.set(i, new Range(range.start+ size, range.end));
                break;
            }

        }

        for (int i = rangeList.size() - 1; i >= 0; i--) {
            Range range = rangeList.get(i);
            if (range == null)
                rangeList.remove(i)  ;
        }
        return result;
    }

    private static int getCurTotalSizeForTask(List<MigrateTask> rangeList) {
        int size = 0;
        for (MigrateTask task : rangeList) {
            size = size + getCurTotalSize(task.getSlots());
        }
        return size;
    }


   public static List<Range> removeAndGetRemain(List<Range> oriRangeList, List<Range> rangeList) {
       for (Range range : rangeList) {
           oriRangeList=removeAndGetRemain(oriRangeList,range) ;
       }
       return oriRangeList;
    }

    private static List<Range> removeAndGetRemain(List<Range> oriRangeList, Range newRange){
        List<Range> result=new ArrayList<>();
        for (Range range : oriRangeList) {
           result.addAll(removeAndGetRemain(range,newRange));
        }
        return result;
    }

    private static List<Range> removeAndGetRemain(Range oriRange, Range newRange) {

        List<Range> result=new ArrayList<>();
        if(newRange.start>oriRange.end||newRange.end<oriRange.start){
            result.add(oriRange);
        } else if(newRange.start<=oriRange.start&&newRange.end>=oriRange.end){
            return result;
        }  else if(newRange.start>oriRange.start&&newRange.end<oriRange.end){
            result.add(new Range(oriRange.start,newRange.start-1)) ;
            result.add(new Range(newRange.end+1,oriRange.end)) ;
        } else if(newRange.start<=oriRange.start&&newRange.end<oriRange.end){
            result.add(new Range(newRange.end+1,oriRange.end)) ;
        } else if(newRange.start>oriRange.start&&newRange.end>=oriRange.end){
            result.add(new Range(oriRange.start,newRange.start-1)) ;
        }


        return result;
    }
    public static  String convertRangeListToString(List<Range> rangeList)
    {   List<String> rangeStringList=new ArrayList<>();
        for (Range range : rangeList) {
            if(range.start==range.end){
                rangeStringList.add(String.valueOf(range.start))  ;
            } else{
                rangeStringList.add(range.start+"-"+range.end)  ;
            }
        }
     return    Joiner.on(',').join(rangeStringList);
    }

    public static List<Range>  convertRangeStringToList(String rangeStr){
        List<String> ranges = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(rangeStr);
        List<Range> rangeList = new ArrayList<>();
        for (String range : ranges) {
            List<String> vv = Splitter.on("-").omitEmptyStrings().trimResults().splitToList(range);
            if (vv.size() == 2) {
                Range ran = new Range(Integer.parseInt(vv.get(0)), Integer.parseInt(vv.get(1)));
                rangeList.add(ran);

            } else if (vv.size() == 1) {
                Range ran = new Range(Integer.parseInt(vv.get(0)), Integer.parseInt(vv.get(0)));
                rangeList.add(ran);

            } else {
                throw new RuntimeException("load crc32slot datafile error:dn=value=" + range);
            }
        }
        return rangeList;
    }

    public static int getCurTotalSize(List<Range> rangeList) {
        int size = 0;
        for (Range range : rangeList) {
            size = size + range.size;
        }
        return size;
    }

    public static String getDatabaseFromDataNode(String dn){
        return    MycatServer.getInstance().getConfig().getDataNodes().get(dn).getDatabase();
    }
    public static String getDataHostFromDataNode(String dn){
        return    MycatServer.getInstance().getConfig().getDataNodes().get(dn).getDbPool().getHostName();
    }
    public static List<Range> convertAllTask(List<MigrateTask> allTasks){
        List<Range>  resutlList=new ArrayList<>();
        for (MigrateTask allTask : allTasks) {
            resutlList.addAll(allTask.getSlots());
        }
        return resutlList;
    }
    public static List<MigrateTask> queryAllTask(String basePath, List<String> dataHost) throws Exception {
        List<MigrateTask>  resutlList=new ArrayList<>();
        for (String dataHostName : dataHost) {
            if("_prepare".equals(dataHostName)||"_commit".equals(dataHostName)||"_clean".equals(dataHostName))
                continue;
            resutlList.addAll(  JSON
                    .parseArray(new String(ZKUtils.getConnection().getData().forPath(basePath+"/"+dataHostName),"UTF-8") ,MigrateTask.class));
        }
        return resutlList;
    }

    public static String makeCountSql(MigrateTask task){
        StringBuilder sb=new StringBuilder();
        sb.append("select count(*) as count from ");
        sb.append(task.getTable()).append(" where ");
        List<Range> slots = task.getSlots();
        for (int i = 0; i < slots.size(); i++) {
            Range range = slots.get(i);
            if(i!=0)
                sb.append(" or ");
            if(range.start==range.end){
                sb.append(" _slot=").append(range.start);
            }   else {
                sb.append(" (_slot>=").append(range.start);
                sb.append(" and _slot<=").append(range.end).append(")");
            }
        }
        return sb.toString();
    }

    public static void execulteSql(String sql,String toDn) throws SQLException, IOException {
        PhysicalDBNode dbNode = MycatServer.getInstance().getConfig().getDataNodes().get(toDn);
        PhysicalDBPool dbPool = dbNode.getDbPool();
        PhysicalDatasource datasource = dbPool.getSources()[dbPool.getActivedIndex()];
        DBHostConfig config = datasource.getConfig();
        Connection con = null;
        try {
            con =  DriverManager
                    .getConnection("jdbc:mysql://"+config.getUrl()+"/"+dbNode.getDatabase(),config.getUser(),config.getPassword());

            JdbcUtils.execute(con,sql, new ArrayList<>());

        } finally{
            JdbcUtils.close(con);
        }

    }
    public static long execulteCount(String sql,String toDn) throws SQLException, IOException {
        PhysicalDBNode dbNode = MycatServer.getInstance().getConfig().getDataNodes().get(toDn);
        PhysicalDBPool dbPool = dbNode.getDbPool();
        PhysicalDatasource datasource = dbPool.getSources()[dbPool.getActivedIndex()];
        DBHostConfig config = datasource.getConfig();
        Connection con = null;
        try {
            con =  DriverManager.getConnection("jdbc:mysql://"+config.getUrl()+"/"+dbNode.getDatabase(),config.getUser(),config.getPassword());

            List<Map<String, Object>> result=      JdbcUtils.executeQuery(con,sql, new ArrayList<>());
            if(result.size()==1){
                return (long) result.get(0).get("count");
            }
        } finally{
            JdbcUtils.close(con);
        }
        return 0;
    }
}
