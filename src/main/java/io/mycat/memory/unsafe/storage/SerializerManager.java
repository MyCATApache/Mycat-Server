package io.mycat.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by zagnix on 2016/6/3.
 */
public class SerializerManager  {

    /**
     * Wrap an output stream for compression if block compression is enabled for its block type
     */
    public  OutputStream wrapForCompression(ConnectionId blockId , OutputStream s){
        return  s;
    }

    /**
     * Wrap an input stream for compression if block compression is enabled for its block type
     */
    public InputStream wrapForCompression(ConnectionId blockId, InputStream s){
        return  s;
    }

}
