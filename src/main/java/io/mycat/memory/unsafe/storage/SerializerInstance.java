package io.mycat.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by zagnix on 2016/6/3.
 */
public abstract  class SerializerInstance {
    protected abstract SerializationStream serializeStream(OutputStream s );
    protected abstract DeserializationStream deserializeStream(InputStream s);
}
