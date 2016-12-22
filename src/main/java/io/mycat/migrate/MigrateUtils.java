package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.MycatServer;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.util.ZKUtils;

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

    public static List<PartitionByCRC32PreSlot.Range> convertAllTask(List<MigrateTask> allTasks){
        List<PartitionByCRC32PreSlot.Range>  resutlList=new ArrayList<>();
        for (MigrateTask allTask : allTasks) {
            resutlList.addAll(allTask.getSlots());
        }
        return resutlList;
    }
    public static List<MigrateTask> queryAllTask(String basePath, List<String> dataHost) throws Exception {
        List<MigrateTask>  resutlList=new ArrayList<>();
        for (String dataHostName : dataHost) {
            resutlList.addAll(  JSON
                    .parseArray(new String(ZKUtils.getConnection().getData().forPath(basePath+"/"+dataHostName),"UTF-8") ,MigrateTask.class));
        }
        return resutlList;
    }
}
