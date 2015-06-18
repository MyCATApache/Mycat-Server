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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;

public class CircularArrayList<E>
        extends AbstractList<E> implements RandomAccess {
  
    private final int n; // buffer length
    private final List<E> buf; // a List implementing RandomAccess
    private int head = 0;
    private int tail = 0;
  
    public CircularArrayList(int capacity) {
        n = capacity + 1;
        buf = new ArrayList<E>(Collections.nCopies(n, (E) null));
    }
  
    public int capacity() {
        return n - 1;
    }
  
    private int wrapIndex(int i) {
        int m = i % n;
        if (m < 0) { // java modulus can be negative
            m += n;
        }
        return m;
    }
  
    // This method is O(n) but will never be called if the
    // CircularArrayList is used in its typical/intended role.
    private void shiftBlock(int startIndex, int endIndex) {
        assert (endIndex > startIndex);
        for (int i = endIndex - 1; i >= startIndex; i--) {
            set(i + 1, get(i));
        }
    }
  
    @Override
    public int size() {
        return tail - head + (tail < head ? n : 0);
    }
  
    @Override
    public E get(int i) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf.get(wrapIndex(head + i));
    }
  
    @Override
    public E set(int i, E e) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
        return buf.set(wrapIndex(head + i), e);
    }
  
    @Override
    public void add(int i, E e) {
        int s = size();
        if (s == n - 1) {
            throw new IllegalStateException("Cannot add element."
                    + " CircularArrayList is filled to capacity.");
        }
        
        if (i < 0 || i > s) {
            throw new IndexOutOfBoundsException();
        }
        tail = wrapIndex(tail + 1);
        if (i < s) {
            shiftBlock(i, s);
        }
        set(i, e);
    }
  
    @Override
    public E remove(int i) {
        int s = size();
        if (i < 0 || i >= s) {
            throw new IndexOutOfBoundsException();
        }
        E e = get(i);
        if (i > 0) {
            shiftBlock(0, i);
        }
        head = wrapIndex(head + 1);
        return e;
    }
}