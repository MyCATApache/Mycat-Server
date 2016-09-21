package io.mycat.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * @author CrazyPig
 * @since 2016-09-09
 *
 */
public class HexFormatUtilTest {
	
	@Test
	public void testBytesToString() {
		byte[] bytes = new byte[]{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
		};
		String hexString = HexFormatUtil.bytesToHexString(bytes);
		String expected = "0102030405060708090A0B0C0D0E0F1011121314";
		Assert.assertEquals(expected, hexString);
	}

}
