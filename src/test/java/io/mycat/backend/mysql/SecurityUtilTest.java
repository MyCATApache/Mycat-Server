package io.mycat.backend.mysql;

import org.junit.Test;

/**
 * Created by liuwei on 16/7/15.
 */
public class SecurityUtilTest {

    @Test
    public void testScramble411AuthByPassword() throws Exception {
        byte[] seed=SecurityUtil.StringToBytes("A4292CE69F53CA03730319B68BEBEFEF5B680C55");
        byte[] message=SecurityUtil.scramble411("abc".getBytes(),seed);
        byte[] pw=SecurityUtil.scramble411Password("abc".getBytes());
        assert SecurityUtil.bytesToString(pw).equals("0D3CED9BEC10A777AEC23CCC353A8C08A633045E");

        assert SecurityUtil.bytesToString(pw).equals(SecurityUtil.bytesToString(SecurityUtil.scramble411AuthByPassword(pw,message,seed)));
    }
}