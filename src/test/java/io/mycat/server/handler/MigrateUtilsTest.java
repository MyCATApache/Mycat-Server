package io.mycat.server.handler;

import com.google.common.collect.Lists;
import io.mycat.migrate.MigrateTask;
import io.mycat.migrate.MigrateUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static io.mycat.route.function.PartitionByCRC32PreSlot.Range;

/**
 * Created by magicdoom on 2016/9/16.
 */
public class MigrateUtilsTest {
    @Test
    public void test1()
    {   String table="test";
        Map<Integer, List<Range>> integerListMap = new TreeMap<>();
        integerListMap.put(0,Lists.newArrayList(new Range(0,32))) ;
        integerListMap.put(1,Lists.newArrayList(new Range(33,65))) ;
        integerListMap.put(2,Lists.newArrayList(new Range(66,99))) ;
        pringList("beforse  balance :",integerListMap);
        //dn1=0-32    dn2=33-65  dn3=66-99
        int totalSlots=100;
        List<String> oldDataNodes = Lists.newArrayList("dn1","dn2","dn3");
        List<String> newDataNodes =  Lists.newArrayList("dn4","dn5");
        Map<String, List<MigrateTask>> tasks= MigrateUtils
                .balanceExpand(table, integerListMap, oldDataNodes, newDataNodes,totalSlots);
        for (Map.Entry<String, List<MigrateTask>> stringListEntry : tasks.entrySet()) {
            String key=stringListEntry.getKey();
            List<Range> rangeList=new ArrayList<>();
            List<MigrateTask> value=stringListEntry.getValue();
            for (MigrateTask task : value) {
               rangeList.addAll(task.slots);
            }
            Assert.assertEquals(true,value.size()==2);
               if("dn4".equals(key)) {
                Assert.assertEquals(0, rangeList.get(0).start);
                Assert.assertEquals(12, rangeList.get(0).end);
                Assert.assertEquals(33, rangeList.get(1).start);
                Assert.assertEquals(39, rangeList.get(1).end);
            }  else   if("dn5".equals(key)) {
                   Assert.assertEquals(40, rangeList.get(0).start);
                   Assert.assertEquals(45, rangeList.get(0).end);
                   Assert.assertEquals(66, rangeList.get(1).start);
                   Assert.assertEquals(79, rangeList.get(1).end);
               }
            integerListMap.put(integerListMap.size(),rangeList);
        }

        pringList("after balance :",integerListMap);

        System.out.println("agin balance .....................");




         oldDataNodes = Lists.newArrayList("dn1","dn2","dn3","dn4","dn5");
         newDataNodes =  Lists.newArrayList("dn6","dn7","dn8","dn9");
        Map<String, List<MigrateTask>> tasks1= MigrateUtils.balanceExpand(table, integerListMap, oldDataNodes, newDataNodes,totalSlots);
        for (Map.Entry<String, List<MigrateTask>> stringListEntry : tasks1.entrySet()) {
            String key=stringListEntry.getKey();
            List<Range> rangeList=new ArrayList<>();
            List<MigrateTask> value=stringListEntry.getValue();
            for (MigrateTask task : value) {
                rangeList.addAll(task.slots);
            }
            if("dn6".equals(key)) {
                Assert.assertEquals(13, rangeList.get(0).start);
                Assert.assertEquals(21, rangeList.get(0).end);
                Assert.assertEquals(46, rangeList.get(1).start);
                Assert.assertEquals(48, rangeList.get(1).end);
            }  else   if("dn7".equals(key)) {
                Assert.assertEquals(49, rangeList.get(0).start);
                Assert.assertEquals(54, rangeList.get(0).end);
                Assert.assertEquals(80, rangeList.get(1).start);
                Assert.assertEquals(84, rangeList.get(1).end);
            } else     if("dn8".equals(key)) {
                Assert.assertEquals(85, rangeList.get(0).start);
                Assert.assertEquals(88, rangeList.get(0).end);
                Assert.assertEquals(0, rangeList.get(1).start);
                Assert.assertEquals(6, rangeList.get(1).end);
            }  else   if("dn9".equals(key)) {
                Assert.assertEquals(7, rangeList.get(0).start);
                Assert.assertEquals(8, rangeList.get(0).end);
                Assert.assertEquals(40, rangeList.get(1).start);
                Assert.assertEquals(45, rangeList.get(1).end);
            }

            integerListMap.put(integerListMap.size(),rangeList);
        }

        pringList("agin balance :",integerListMap);


        oldDataNodes = Lists.newArrayList("dn1","dn2","dn3","dn4","dn5","dn6","dn7","dn8","dn9");
        newDataNodes =  Lists.newArrayList("dn10","dn11","dn12","dn13","dn14","dn15","dn16","dn17","dn18","dn19","dn20","dn21","dn22","dn23","dn24","dn25","dn26","dn27","dn28","dn29","dn30","dn31","dn32","dn33","dn34","dn35","dn36","dn37","dn38","dn39","dn40","dn41","dn42","dn43","dn44","dn45","dn46","dn47","dn48","dn49","dn50","dn51","dn52","dn53","dn54","dn55","dn56","dn57","dn58","dn59","dn60","dn61","dn62","dn63","dn64","dn65","dn66","dn67","dn68","dn69","dn70","dn71","dn72","dn73","dn74","dn75","dn76","dn77","dn78","dn79","dn80","dn81","dn82","dn83","dn84","dn85","dn86","dn87","dn88","dn89","dn90","dn91","dn92","dn93","dn94","dn95","dn96","dn97","dn98","dn99","dn100"
                );
        Map<String, List<MigrateTask>> tasks2= MigrateUtils.balanceExpand(table, integerListMap, oldDataNodes, newDataNodes,totalSlots);
        for (Map.Entry<String, List<MigrateTask>> stringListEntry : tasks2.entrySet()) {
            String key=stringListEntry.getKey();
            List<Range> rangeList=new ArrayList<>();
            List<MigrateTask> value=stringListEntry.getValue();
            for (MigrateTask task : value) {
                rangeList.addAll(task.slots);
            }

            if("dn10".equals(key)) {
                Assert.assertEquals(22, rangeList.get(0).start);
                Assert.assertEquals(22, rangeList.get(0).end);
            }  else   if("dn100".equals(key)) {
                Assert.assertEquals(83, rangeList.get(0).start);
                Assert.assertEquals(83, rangeList.get(0).end);
            } else     if("dn50".equals(key)) {
                Assert.assertEquals(69, rangeList.get(0).start);
                Assert.assertEquals(69, rangeList.get(0).end);
            }  else   if("dn99".equals(key)) {
                Assert.assertEquals(82, rangeList.get(0).start);
                Assert.assertEquals(82, rangeList.get(0).end);

            }
            integerListMap.put(integerListMap.size(),rangeList);
        }

        pringList("agin agin balance :",integerListMap);

    }

    private void pringList(String comm,Map<Integer, List<Range>> integerListMap) {
        System.out.println(comm);
        for (Map.Entry<Integer, List<Range>> integerListEntry : integerListMap.entrySet()) {
            Integer key=integerListEntry.getKey();
            List<Range> value=integerListEntry.getValue();
            System.out.println(key+":"+listToString(value)+":"+MigrateUtils.getCurTotalSize(value));
        }
    }

    private String listToString(List<Range> rangeList)
    {  String rtn="";
        for (Range range : rangeList) {
            rtn=rtn+range.start+"-"+range.end+",";
        }

        return rtn;
    }

}
