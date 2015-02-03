package org.opencloudb.route.function;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.opencloudb.util.SplitUtil;

public class TestLatestMonthPartion {

	@Test
	public void testSetDataNodes() {
		String dn = "dn$4-5";
		String theDataNodes[] = SplitUtil.split(dn, ',', '$', '-', '[', ']');
		System.out.println(Arrays.toString(theDataNodes));
		LatestMonthPartion partion = new LatestMonthPartion();
		partion.setSplitOneDay(24);
		Integer val = partion.calculate("2015020100");
		assertTrue(val == 0);
		val = partion.calculate("2015020216");
		assertTrue(val == 40);
		val = partion.calculate("2015022823");
		assertTrue(val == 27 * 24 + 23);
	}

}
