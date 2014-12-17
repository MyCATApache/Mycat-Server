package org.opencloudb.mpp.tmp;

import java.util.Vector;

import org.opencloudb.net.mysql.RowDataPacket;

/**
 * 
 * @author coderczp-2014-12-17
 */
public interface HeapItf {

    public abstract void buildMinHeap();

    public abstract RowDataPacket getRoot();

    public abstract void add(RowDataPacket row);

    public abstract Vector<RowDataPacket> getData();

    public abstract void setRoot(RowDataPacket root);

    public abstract void addIfRequired(RowDataPacket row);


}
