package io.mycat.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * 序列化管理器
 * Created by zagnix on 2016/6/3.
 */
public class SerializerManager  {

    /**
     * Wrap an output stream for compression if block compression is enabled for its block type
     * 如果为其块类型启用了块压缩，则将输出流进行压缩
     */
    public OutputStream wrapForCompression(ConnectionId blockId , OutputStream s){
        return  s;
    }

    /**
     * Wrap an input stream for compression if block compression is enabled for its block type
     * 如果对块类型启用块压缩，则封装输入流进行压缩
     */
    public InputStream wrapForCompression(ConnectionId blockId, InputStream s){
        return  s;
    }

}
