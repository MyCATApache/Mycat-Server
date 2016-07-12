package io.mycat.memory.unsafe.row;


import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Created by zagnix on 2016/6/27.
 */
public class UnsafeRowList {
    @Test
    public  void testUnsafeRowList(){
        ArrayList<UnsafeRow> list = new ArrayList<UnsafeRow>();
        UnsafeRow unsafeRow  ;
        BufferHolder bufferHolder ;
        UnsafeRowWriter unsafeRowWriter;
        String line = "testUnsafeRow";

        for (int i = 0; i <10; i++) {
            unsafeRow = new UnsafeRow(3);
            bufferHolder = new BufferHolder(unsafeRow);
            unsafeRowWriter = new UnsafeRowWriter(bufferHolder,3);
            bufferHolder.reset();

            unsafeRow.setInt(0,89);
            unsafeRowWriter.write(1,line.getBytes(),0,line.length());
            unsafeRow.setInt(2,23);

            unsafeRow.setTotalSize(bufferHolder.totalSize());
            list.add(unsafeRow);
        }


        for (int i = 0; i <10; i++) {
            UnsafeRow row = list.get(i);
            row.setInt(0,1000+i);
        }


        for (int i = 0; i <10; i++) {
            UnsafeRow row = list.get(i);
           Assert.assertEquals(1000+i,row.getInt(0));
        }

    }
}
