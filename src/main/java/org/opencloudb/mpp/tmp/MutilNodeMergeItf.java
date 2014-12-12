package org.opencloudb.mpp.tmp;

import java.util.Collection;

import org.opencloudb.net.mysql.RowDataPacket;

public interface MutilNodeMergeItf {

    public static final String SWAP_PATH = "./";

    public abstract void addRow(RowDataPacket row);

    public abstract Collection<RowDataPacket> getResult();

    //必须在finally里调用此方法
    public abstract void close();

}