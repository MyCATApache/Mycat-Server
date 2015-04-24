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
package org.opencloudb.queue;

/**
 * @author mycat
 */
public final class Queue<T> {

    private final static int MIN_SHRINK_SIZE = 1024;

    private T[] items;
    private int count = 0;
    private int start = 0, end = 0;
    private int suggestedSize, size = 0;

    public Queue(int suggestedSize) {
        this.size = this.suggestedSize = suggestedSize;
        items = newArray(this.size);
    }

    public Queue() {
        this(4);
    }

    public synchronized void clear() {
        count = start = end = 0;
        size = suggestedSize;
        items = newArray(size);
    }

    public synchronized boolean hasElements() {
        return (count != 0);
    }

    public synchronized int size() {
        return count;
    }

    public synchronized void prepend(T item) {
        if (count == size) {
            makeMoreRoom();
        }
        if (start == 0) {
            start = size - 1;
        } else {
            start--;
        }
        this.items[start] = item;
        count++;
        if (count == 1) {
            notify();
        }
    }

    public synchronized void append(T item) {
        append0(item, count == 0);
    }

    public synchronized void appendSilent(T item) {
        append0(item, false);
    }

    public synchronized void appendLoud(T item) {
        append0(item, true);
    }

    public synchronized T getNonBlocking() {
        if (count == 0) {
            return null;
        }
        // pull the object off, and clear our reference to it
        T retval = items[start];
        items[start] = null;
        start = (start + 1) % size;
        count--;
        return retval;
    }

    public synchronized void waitForItem() {
        while (count == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }
    }

    public synchronized T get(long maxwait) {
        if (count == 0) {
            try {
                wait(maxwait);
            } catch (InterruptedException e) {
            }
            if (count == 0) {
                return null;
            }
        }
        return get();
    }

    public synchronized T get() {
        while (count == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
            }
        }

        // pull the object off, and clear our reference to it
        T retval = items[start];
        items[start] = null;

        start = (start + 1) % size;
        count--;

        // if we are only filling 1/8th of the space, shrink by half
        if ((size > MIN_SHRINK_SIZE) && (size > suggestedSize) && (count < (size >> 3))) {
            shrink();
        }

        return retval;
    }

    private void append0(T item, boolean notify) {
        if (count == size) {
            makeMoreRoom();
        }
        this.items[end] = item;
        end = (end + 1) % size;
        count++;
        if (notify) {
            notify();
        }
    }

    private void makeMoreRoom() {
        T[] items = newArray(size * 2);
        System.arraycopy(this.items, start, items, 0, size - start);
        System.arraycopy(this.items, 0, items, size - start, end);
        start = 0;
        end = size;
        size *= 2;
        this.items = items;
    }

    // shrink by half
    private void shrink() {
        T[] items = newArray(size / 2);
        if (start > end) {
            // the data wraps around
            System.arraycopy(this.items, start, items, 0, size - start);
            System.arraycopy(this.items, 0, items, size - start, end + 1);
        } else {
            // the data does not wrap around
            System.arraycopy(this.items, start, items, 0, end - start + 1);
        }
        size = size / 2;
        start = 0;
        end = count;
        this.items = items;
    }

    @SuppressWarnings("unchecked")
    private T[] newArray(int size) {
        return (T[]) new Object[size];
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[count=").append(count);
        buf.append(", size=").append(size);
        buf.append(", start=").append(start);
        buf.append(", end=").append(end);
        buf.append(", elements={");
        for (int i = 0; i < count; i++) {
            int pos = (i + start) % size;
            if (i > 0)
                buf.append(", ");
            buf.append(items[pos]);
        }
        buf.append("}]");
        return buf.toString();
    }

}