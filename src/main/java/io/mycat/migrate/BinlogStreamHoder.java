package io.mycat.migrate;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by magicdoom on 2016/12/25.
 */
public class BinlogStreamHoder {
    public static ConcurrentMap<String,BinlogStream> binlogStreamMap=new ConcurrentHashMap<>();
}
