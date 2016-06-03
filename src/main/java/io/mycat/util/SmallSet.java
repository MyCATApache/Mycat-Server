/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.util;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * usually one element
 * 
 * @author mycat
 */
public final class SmallSet<E> extends AbstractSet<E> implements Set<E>, Cloneable, Serializable {

    private static final long serialVersionUID = 2037649294658559180L;

    private final int initSize;
    private ArrayList<E> list;
    private E single;
    private int size;

    public SmallSet() {
        this(2);
    }

    public SmallSet(int initSize) {
        this.initSize = initSize;
    }

    @Override
    public boolean add(E e) {
        switch (size) {
        case 0:
            ++size;
            single = e;
            return true;
        case 1:
            if (isEquals(e, single)) {
                return false;
            }
            list = new ArrayList<E>(initSize);
            list.add(single);
            list.add(e);
            ++size;
            return true;
        default:
            for (int i = 0; i < list.size(); ++i) {
                E e1 = list.get(i);
                if (isEquals(e1, e)) {
                    return false;
                }
            }
            list.add(e);
            ++size;
            return true;
        }
    }

    private boolean isEquals(E e1, E e2) {
        if (e1 == null) {
            return e2 == null;
        }
        return e1.equals(e2);
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int i;
            private boolean next;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public E next() {
                next = true;
                switch (size) {
                case 0:
                    throw new NoSuchElementException();
                case 1:
                    switch (i) {
                    case 0:
                        ++i;
                        return single;
                    default:
                        throw new NoSuchElementException();
                    }
                default:
                    try {
                        E e = list.get(i);
                        ++i;
                        return e;
                    } catch (IndexOutOfBoundsException e) {
                        throw new NoSuchElementException(e.getMessage());
                    }
                }
            }

            @Override
            public void remove() {
                if (!next) {
                    throw new IllegalStateException();
                }
                switch (size) {
                case 0:
                    throw new IllegalStateException();
                case 1:
                    size = i = 0;
                    single = null;
                    if (list != null && !list.isEmpty()) {
                        list.remove(0);
                    }
                    break;
                default:
                    list.remove(--i);
                    if (--size == 1) {
                        single = list.get(0);
                    }
                    break;
                }
                next = false;
            }
        };
    }

    @Override
    public int size() {
        return size;
    }

}