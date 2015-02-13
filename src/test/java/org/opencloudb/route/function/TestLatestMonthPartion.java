package org.opencloudb.route.function;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestLatestMonthPartion {

	@Test
	public void testSetDataNodes() {
		LatestMonthPartion partion = new LatestMonthPartion();
		partion.setSplitOneDay(24);
		Integer val = partion.calculate("2015020100");
		assertTrue(val == 0);
		val = partion.calculate("2015020216");
		assertTrue(val == 40);
		val = partion.calculate("2015022823");
		assertTrue(val == 27 * 24 + 23);

		Integer[] span = partion.calculateRange("2015020100", "2015022823");
		assertTrue(span.length == 27 * 24 + 23 + 1);
		assertTrue(span[0] == 0 && span[span.length - 1] == 27 * 24 + 23);
		
		
		span = partion.calculateRange("2015020100", "2015020123");
		assertTrue(span.length == 24);
		assertTrue(span[0] == 0 && span[span.length - 1] == 23);
	}

}
