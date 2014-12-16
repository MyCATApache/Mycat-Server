package org.opencloudb.mpp.tmp;

import java.util.Vector;

import org.opencloudb.net.mysql.RowDataPacket;

public interface HeapItf {

    public abstract void buildMinHeap();

    public abstract RowDataPacket getRoot();

    public abstract void setRoot(RowDataPacket root);

    public abstract Vector<RowDataPacket> getData();

    public abstract void add(RowDataPacket row);

}