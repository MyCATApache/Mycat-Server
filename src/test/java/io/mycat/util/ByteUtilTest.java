package io.mycat.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * <p>
 * COPYRIGHT © 2001 - 2016 VOYAGE ONE GROUP INC. ALL RIGHTS RESERVED.
 *
 * @author vantis 2017/9/29
 * @version 1.0.0
 */
public class ByteUtilTest {
    @Test
    public void compareNumberByte() throws Exception {
        byte[] b1 = {'1', '.', '2'};
        byte[] b2 = {'1', '2', '2'};
        int i = ByteUtil.compareNumberByte(b1, b2);
    }

}