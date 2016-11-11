package io.mycat.memory.unsafe.row;


import junit.framework.Assert;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

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
    
    public void testUnsafeRowWithDecimal() {
     	
     	int fieldCount = 4;
     	
     	String value = "12345678901234567890123456789.0123456789";
     	String value1 = "100";
     	BigDecimal decimal = new BigDecimal(value);
     	BigDecimal decimal1 = new BigDecimal(value1);
     	System.out.println("decimal precision : " + decimal.precision() + ", scale : " + decimal.scale());
     	
     	UnsafeRow unsafeRow = new UnsafeRow(fieldCount);
         BufferHolder bufferHolder = new BufferHolder(unsafeRow,64);
         UnsafeRowWriter unsafeRowWriter = new UnsafeRowWriter(bufferHolder,fieldCount);
         bufferHolder.reset();
         
         unsafeRow.setInt(0, 100);
         unsafeRow.setDouble(1, 0.99);
         unsafeRow.setLong(2, 1000);
         unsafeRowWriter.write(3, decimal);
         
         assertEquals(100, unsafeRow.getInt(0));
         assertEquals("0.99", String.valueOf(unsafeRow.getDouble(1)));
         assertEquals(1000, unsafeRow.getLong(2));
         assertEquals(decimal, unsafeRow.getDecimal(3, decimal.scale()));
         
         unsafeRow.updateDecimal(3, decimal1);
         assertEquals(decimal1, unsafeRow.getDecimal(3, decimal1.scale()));
         
         // update null decimal
         BigDecimal nullDecimal = null;
         unsafeRow.updateDecimal(3, nullDecimal);
         assertEquals(nullDecimal, unsafeRow.getDecimal(3, 0));
         
         unsafeRow.updateDecimal(3, decimal);
         assertEquals(decimal, unsafeRow.getDecimal(3, decimal.scale()));
         
     }


//    @Test
//    public void  testUnsafeRowInsert(){
//        UnsafeRow unsafeRow = new UnsafeRow(4);
//
//        assert(unsafeRow.getFloat(0) == 7.4f);
//        assert(unsafeRow.getInt(1) == 9);
//        assert(unsafeRow.getLong(2) == 455555);
//        Assert.assertEquals("testUnsafeRow3",new String(unsafeRow.getBinary(3)));
//    }

};
