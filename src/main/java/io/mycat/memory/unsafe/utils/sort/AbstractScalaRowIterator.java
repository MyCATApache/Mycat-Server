package io.mycat.memory.unsafe.utils.sort;

import java.util.Iterator;

/**
 * Created by zagnix 2016/6/6.
 */
public class AbstractScalaRowIterator<T> implements Iterator<T> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }

    @Override
    public void remove() {

    }
}
