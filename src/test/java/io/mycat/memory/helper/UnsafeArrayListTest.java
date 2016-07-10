/**
 * Tests the UnsafeArrayList using guava-testlib.
 * Used https://www.klittlepage.com/2014/01/08/testing-collections-with-guava-testlib-and-junit-4/
 * as a reference.
 */
package io.mycat.memory.helper;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.junit.Assert.assertEquals;

public class UnsafeArrayListTest {

  /**
   * Add your additional map cases here.
   */
  public static class AdditionalTests {

    static final int TEST_SIZE = 25; // Recommended larger than UnsafeArrayList.DEFAULT_CAPACITY

    UnsafeArrayList<LongPoint> list;

    @Before
    public void setup() {
      list = new UnsafeArrayList<>(LongPoint.class);
      for (int i = 0; i < TEST_SIZE; i++) {
        list.add(new LongPoint(i * 2, i * 2 + 1));
      }
      assertEquals(TEST_SIZE, list.size());
    }

    @Test public void testGet() throws Exception {
      for (int i = 0; i < TEST_SIZE; i++) {
        assertEquals(new LongPoint(i * 2, i * 2 + 1), list.get(i));
      }
    }

    @Test public void testGetInPlace() throws Exception {
      LongPoint tmp = new LongPoint(-1, -1);
      for (int i = 0; i < TEST_SIZE; i++) {
        list.get(tmp, i);
        assertEquals(new LongPoint(i * 2, i * 2 + 1), tmp);
      }
    }

  }

}
