package org.opencloudb.route.function;

import java.math.BigInteger;

/**
 * Created by Hash Zhang on 2016/4/14.
 */
public class PartitionByHashMod extends PartitionByMod {
    protected boolean watch = false;

    /**
     * Using Wang/Jenkins Hash
     *
     * @param key
     * @return hash value
     */
    protected int hash(int key) {
        key = (~key) + (key << 21); // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8); // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4); // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
        return key;
    }

    @Override
    public Integer calculate(String columnValue) {
        columnValue = columnValue.replace("\'", " ");
        columnValue = columnValue.trim();
        BigInteger bigNum = new BigInteger(hash(columnValue.hashCode()) + "").abs();
        // if count==2^n, then m%count == m&(count-1)
        if (watch) {
            return bigNum.intValue() & (count - 1);
        }
        return (bigNum.mod(BigInteger.valueOf(count))).intValue();
    }

    @Override
    public void init() {
        super.init();
    }

    @Override
    public void setCount(int count) {
        this.count = count;
        if ((count & (count - 1)) == 0) {
            watch = true;
        }
    }

}
