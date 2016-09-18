package io.mycat.server.handler;

import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by magicdoom on 2016/9/15.
 */
public class MigrateTask {

    public String from;
    public String to;
    public String table;
    public List<Range> slots=new ArrayList<>();

    public String method;
    public String fclass;


    public int getSize()
    {   int size=0;
        for (Range slot : slots) {
           size=size+slot.size;
        }
        return size;
    }

    public void addSlots(Range range)
    {
        slots.add(range);
    }

    public void addSlots(List<Range> ranges)
    {
        slots.addAll(ranges);
    }
}
