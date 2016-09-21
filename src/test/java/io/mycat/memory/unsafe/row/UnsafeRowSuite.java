package io.mycat.memory.unsafe.row;


import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by zagnix on 2016/6/10.
 */
public class UnsafeRowSuite {


    @Test
    public void  testUnsafeRowSingle(){
        UnsafeRow unsafeRow = new UnsafeRow(5);
        BufferHolder bufferHolder = new BufferHolder(unsafeRow,64);
        UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder,5);
        bufferHolder.reset();

        String line2 = "testUnsafeRow3";
        unsafeRow.setFloat(0, 7.4f);
        unsafeRow.setInt(1, 7);
        unsafeRow.setLong(2,455555);
        unsafeRowWriter.write(3,line2.getBytes(),0, line2.length());
        unsafeRow.setNullAt(4);

        unsafeRow.setInt(1, 9);

        assert(unsafeRow.getFloat(0) == 7.4f);
        assert(unsafeRow.getInt(1) == 9);
        assert(unsafeRow.getLong(2) == 455555);
        Assert.assertEquals("testUnsafeRow3",new String(unsafeRow.getBinary(3)));
        assert (false==unsafeRow.isNullAt(3));
        assert (true==unsafeRow.isNullAt(4));
    }


    @Test
    public void  testUnsafeRowInsert(){
        UnsafeRow unsafeRow = new UnsafeRow(4);

        assert(unsafeRow.getFloat(0) == 7.4f);
        assert(unsafeRow.getInt(1) == 9);
        assert(unsafeRow.getLong(2) == 455555);
        Assert.assertEquals("testUnsafeRow3",new String(unsafeRow.getBinary(3)));
    }

};
