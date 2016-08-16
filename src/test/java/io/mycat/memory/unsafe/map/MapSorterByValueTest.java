package io.mycat.memory.unsafe.map;

import org.junit.Test;

import java.util.*;

/**
 * Created by znix on 2016/7/4.
 */
public class MapSorterByValueTest {
    @Test
    public void testMapSorterByValue(){
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("q",23);
        map.put("b",4);
        map.put("c",5);
        map.put("d",6);

        Map<String, Integer> resultMap = mapSorterByValue(map); //按Value进行排序

        for (Map.Entry<String,Integer> entry : resultMap.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    private Map<String,Integer> mapSorterByValue(Map<String,Integer> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }

        Map<String,Integer> sortedMap = new LinkedHashMap<String, Integer>();

        List<Map.Entry<String, Integer>> entryList = new ArrayList<
                Map.Entry<String, Integer>>(
                map.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        Iterator<Map.Entry<String, Integer>> iter = entryList.iterator();
        Map.Entry<String, Integer> tmpEntry = null;
        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
        }
        return sortedMap;
    }
}


