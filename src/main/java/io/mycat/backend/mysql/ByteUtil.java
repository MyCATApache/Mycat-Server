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
package io.mycat.backend.mysql;

/**
 * @author mycat
 */
public class ByteUtil {

    public static int readUB2(byte[] data, int offset) {
        int i = data[offset] & 0xff;
        i |= (data[++offset] & 0xff) << 8;
        return i;
    }

    public static int readUB3(byte[] data, int offset) {
        int i = data[offset] & 0xff;
        i |= (data[++offset] & 0xff) << 8;
        i |= (data[++offset] & 0xff) << 16;
        return i;
    }

    public static long readUB4(byte[] data, int offset) {
        long l = data[offset] & 0xff;
        l |= (data[++offset] & 0xff) << 8;
        l |= (data[++offset] & 0xff) << 16;
        l |= (data[++offset] & 0xff) << 24;
        return l;
    }

    public static long readLong(byte[] data, int offset) {
        long l = (long) (data[offset] & 0xff);
        l |= (long) (data[++offset] & 0xff) << 8;
        l |= (long) (data[++offset] & 0xff) << 16;
        l |= (long) (data[++offset] & 0xff) << 24;
        l |= (long) (data[++offset] & 0xff) << 32;
        l |= (long) (data[++offset] & 0xff) << 40;
        l |= (long) (data[++offset] & 0xff) << 48;
        l |= (long) (data[++offset] & 0xff) << 56;
        return l;
    }

    public static long readLength(byte[] data, int offset) {
        int length = data[offset++] & 0xff;
        switch (length) {
        case 251:
            return MySQLMessage.NULL_LENGTH;
        case 252:
            return readUB2(data, offset);
        case 253:
            return readUB3(data, offset);
        case 254:
            return readLong(data, offset);
        default:
            return length;
        }
    }

    public static int lengthToZero(byte[] data, int offset) {
        int start = offset;
        for (int i = start; i < data.length; i++) {
            if (data[i] == 0) {
                return (i - start);
            }
        }
        int remaining = data.length - start;
        return remaining > 0 ? remaining : 0;
    }

    public static int decodeLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static int decodeLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

}