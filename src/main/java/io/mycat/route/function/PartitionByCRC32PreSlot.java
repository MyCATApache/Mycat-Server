package io.mycat.route.function;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.rule.RuleAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 自动迁移御用分片算法，预分slot 102400个，映射到dn上，再conf下会保存映射文件，请不要修改
 *
 * @author nange magicdoom@gmail.com
 */
public class PartitionByCRC32PreSlot extends AbstractPartitionAlgorithm
        implements RuleAlgorithm, TableRuleAware, SlotFunction,ReloadFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger("PartitionByCRC32PreSlot");

    public static final int DEFAULT_SLOTS_NUM = 102400;

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    private Map<Integer, List<Range>> rangeMap = new TreeMap<>();

    private int count;
    //slot:index
    private int[] rangeMap2 = new int[DEFAULT_SLOTS_NUM];
    private int slot = -1;

    public Map<Integer, List<Range>> getRangeMap() {
        return rangeMap;
    }

    public void saveSlotMapping(Map<Integer, List<Range>> rangeMap) {
        this.rangeMap = rangeMap;

        Properties prop = new Properties();
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator +"ruledata"+ File.separator + ruleName + ".properties");
        if (file.exists())
            file.delete();
        for (Map.Entry<Integer, List<Range>> integerListEntry : rangeMap.entrySet()) {
            String key = String.valueOf(integerListEntry.getKey());
            List<String> values = new ArrayList<>();
            for (Range range : integerListEntry.getValue()) {
                values.add(range.start + "-" + range.end);
            }
            prop.setProperty(key, Joiner.on(",").join(values));
        }
        try {
            Files.createParentDirs(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try (FileOutputStream out = new FileOutputStream(file)) {
            prop.store(out, "WARNING   !!!Please do not modify or delete this file!!!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Properties loadProps(String name, boolean forceNew) {
        Properties prop = new Properties();
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator +"ruledata"+ File.separator + ruleName + ".properties");
        if (file.exists() && forceNew)
            file.delete();
        if (!file.exists()) {
            prop = genarateP();
            try {
                Files.createParentDirs(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try (FileOutputStream out = new FileOutputStream(file)) {
                prop.store(out, "WARNING   !!!Please do not modify or delete this file!!!");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return prop;
        }

        try (FileInputStream filein = new FileInputStream(file)) {
            prop.load(filein);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return prop;
    }

    private Properties genarateP() {
        int slotSize = DEFAULT_SLOTS_NUM / count;
        Properties prop = new Properties();
        for (int i = 0; i < count; i++) {
            if (i == count - 1) {
                prop.put(String.valueOf(i), i * slotSize + "-" + (DEFAULT_SLOTS_NUM - 1));
            } else {
                prop.put(String.valueOf(i), i * slotSize + "-" + ((i + 1) * slotSize - 1));
            }

        }

        return prop;
    }

    private Map<Integer, List<Range>> convertToMap(Properties p) {
        Map<Integer, List<Range>> map = new TreeMap<>();
        for (Object o : p.keySet()) {
            String k = (String) o;
            String v = p.getProperty(k);
            List<String> ranges = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(v);
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
                    throw new RuntimeException("load crc32slot datafile error:dn=" + k + ",value=" + range);
                }
            }
            map.put(Integer.parseInt(k), rangeList);
        }

        return map;
    }

    @Override public void init() {

        super.init();
        if (ruleName != null) {
            Properties p = loadProps(ruleName, false);
            rangeMap = convertToMap(p);
            hack();
        }
    }

    public void reInit() {

        if (ruleName != null) {
            Properties p = loadProps(ruleName, true);
            rangeMap = convertToMap(p);
            hack();
        }
    }


    private void hack(   )
    {
        //todo   优化
        Iterator<Map.Entry<Integer, List<Range>>> iterator = rangeMap.entrySet().iterator();
        while (iterator
                .hasNext()) {
            Map.Entry<Integer, List<Range>> rangeEntry = iterator.next();
            List<Range> range = rangeEntry.getValue();
            for (Range range1 : range) {
                for(int i=range1.start;i<=range1.end;i++)
                {
                    rangeMap2[i]=rangeEntry.getKey() ;
                }
            }

        }
    }
    /**
     * 节点的数量
     *
     * @param count
     */
    public void setCount(int count) {
        this.count = count;
    }

    @Override public Integer calculate(String columnValue) {
        if (ruleName == null)
            throw new RuntimeException();
        PureJavaCrc32 crc32 = new PureJavaCrc32();
        byte[] bytes = columnValue.getBytes(DEFAULT_CHARSET);
        crc32.update(bytes, 0, bytes.length);
        long x = crc32.getValue();
        int slot = (int) (x % DEFAULT_SLOTS_NUM);
        this.slot = slot;
        return rangeMap2[slot];
//        //todo   优化
//        for (Map.Entry<Integer, List<Range>> rangeEntry : rangeMap.entrySet()) {
//            List<Range> range = rangeEntry.getValue();
//            for (Range range1 : range) {
//                if (slot >= range1.start && slot <= range1.end) {
//                    this.slot = slot;
//                    return rangeEntry.getKey();
//                }
//            }
//
//        }
//        this.slot = slot;
//        int slotSize = DEFAULT_SLOTS_NUM / count;
//
//        int index = slot / slotSize;
//        if (slotSize * count != DEFAULT_SLOTS_NUM && index > count - 1) {
//            index = (count - 1);
//        }
//        return index;
    }

    @Override public int getPartitionNum() {
        return this.count;
    }

    private static void hashTest() throws IOException {
        PartitionByCRC32PreSlot hash = new PartitionByCRC32PreSlot();
        hash.setRuleName("test");
        hash.count = 1024;//分片数

        hash.reInit();
        long start = System.currentTimeMillis();
        int[] bucket = new int[hash.count];

        Map<Integer, List<Integer>> hashed = new HashMap<>();

        int total = 1000_0000;//数据量
        int c = 0;
        for (int i = 100_0000; i < total + 100_0000; i++) {//假设分片键从100万开始
            c++;
            int h = hash.calculate(Integer.toString(i));
            if (h >= hash.count) {
                System.out.println("error:" + h);
            }
            bucket[h]++;
            List<Integer> list = hashed.get(h);
            if (list == null) {
                list = new ArrayList<>();
                hashed.put(h, list);
            }
            list.add(i);
        }
        System.out.println(c + "   " + total);
        double d = 0;
        c = 0;
        int idx = 0;
        System.out.println("index    bucket   ratio");
        for (int i : bucket) {
            d += i / (double) total;
            c += i;
            System.out.println(idx++ + "  " + i + "   " + (i / (double) total));
        }
        System.out.println(d + "  " + c);

        long used = System.currentTimeMillis() - start;

        System.out.println("tps " + total * 1000.0 / used);
        System.out.println("****************************************************");

    }

    public static void main(String[] args) throws IOException {
        hashTest();
    }

    private String tableName;
    private String ruleName;

    @Override public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    @Override public String getTableName() {
        return tableName;
    }

    @Override public String getRuleName() {
        return ruleName;
    }

    @Override public int slotValue() {
        return slot;
    }

    @Override public void reload() {
          init();
    }

    public static class Range implements Serializable {
        public Range(int start, int end) {
            this.start = start;
            this.end = end;
            size = end - start + 1;
        }

        public Range() {
        }

        public int start;
        public int end;

        public int size;
    }
}
