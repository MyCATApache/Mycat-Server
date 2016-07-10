/**
 * Tests the UnsafeArrayList using guava-testlib.
 * Used https://www.klittlepage.com/2014/01/08/testing-collections-with-guava-testlib-and-junit-4/
 * as a reference.
 */
package io.mycat.memory.unsafe.row;

import io.mycat.memory.helper.UnsafeArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


import static org.junit.Assert.assertEquals;


/**
 * We need to use static inner classes as JUnit only allows for empty "holder"
 * suite classes.
 */

public class UnsafeArrayListTest {

  /**
   * Add your additional map cases here.
   */
  public static class AdditionalTests {

    static final int TEST_SIZE = 1000000; // Recommended larger than UnsafeArrayList.DEFAULT_CAPACITY

    UnsafeArrayList<UnsafeRowWriter> list;
    UnsafeRow unsafeRow;
    BufferHolder bufferHolder;
    UnsafeRowWriter unsafeRowWriter;
    String line = "t111111111110000";

    @Before
    public void setup() {

      list = new UnsafeArrayList<>(UnsafeRowWriter.class);
      unsafeRow = new UnsafeRow(10);
      bufferHolder = new BufferHolder(unsafeRow,0);
      unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 10);
      bufferHolder.reset();

      for (int j = 0; j < 10; j++) {
        unsafeRowWriter.write(j,line.getBytes());
      }

      unsafeRow.setTotalSize(bufferHolder.totalSize());


      for (int i = 0; i < TEST_SIZE; i++) {
        list.add(unsafeRowWriter);
      }


      assertEquals(TEST_SIZE, list.size());
    }

    @Test public void testGet() throws Exception {

      unsafeRow = new UnsafeRow(10);
      bufferHolder = new BufferHolder(unsafeRow,0);
      unsafeRowWriter = new UnsafeRowWriter(bufferHolder, 10);
      bufferHolder.reset();

      for (int j = 0; j < 10; j++) {
        unsafeRowWriter.write(j,line.getBytes());
      }

      unsafeRow.setTotalSize(bufferHolder.totalSize());

      for (int i = 0; i < 10; i++) {
        assertEquals(unsafeRowWriter.holder().getRow(),list.get(i).holder().getRow());
      }

      UnsafeRowWriter rowWriter = list.get(1000);
      rowWriter.write(0,"zzzzzzzz".getBytes());

      Assert.assertEquals("zzzzzzzz",new String(rowWriter.holder().getRow().getBinary(0)));
    }

    @Test public void testGetInPlace() throws Exception {

      unsafeRow = new UnsafeRow(10);
      bufferHolder = new BufferHolder(unsafeRow,0);
      UnsafeRowWriter unsafeRowWriter0 = new UnsafeRowWriter(bufferHolder, 10);
      bufferHolder.reset();

      for (int j = 0; j < 10; j++) {
        unsafeRowWriter0.write(j,line.getBytes());
      }

      unsafeRow.setTotalSize(bufferHolder.totalSize());


      unsafeRow = new UnsafeRow(10);
      bufferHolder = new BufferHolder(unsafeRow,0);
      UnsafeRowWriter unsafeRowWriter1 = new UnsafeRowWriter(bufferHolder, 10);
      bufferHolder.reset();

      for (int j = 0; j < 10; j++) {
        unsafeRowWriter1.write(j,line.getBytes());
      }

      unsafeRow.setTotalSize(bufferHolder.totalSize());

      for (int i = 0; i < TEST_SIZE; i++) {
        list.get(unsafeRowWriter0, i);
        assertEquals(unsafeRowWriter1.holder().getRow(), unsafeRowWriter0.holder().getRow());
      }
    }

  }

}
