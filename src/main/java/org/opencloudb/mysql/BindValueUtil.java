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
package org.opencloudb.mysql;

import java.io.UnsupportedEncodingException;

import org.opencloudb.config.Fields;

/**
 * @author mycat
 */
public class BindValueUtil {

    public static final void read(MySQLMessage mm, BindValue bv, String charset) throws UnsupportedEncodingException {
        switch (bv.type & 0xff) {
        case Fields.FIELD_TYPE_BIT:
            bv.value = mm.readBytesWithLength();
            break;
        case Fields.FIELD_TYPE_TINY:
            bv.byteBinding = mm.read();
            break;
        case Fields.FIELD_TYPE_SHORT:
            bv.shortBinding = (short) mm.readUB2();
            break;
        case Fields.FIELD_TYPE_LONG:
            bv.intBinding = mm.readInt();
            break;
        case Fields.FIELD_TYPE_LONGLONG:
            bv.longBinding = mm.readLong();
            break;
        case Fields.FIELD_TYPE_FLOAT:
            bv.floatBinding = mm.readFloat();
            break;
        case Fields.FIELD_TYPE_DOUBLE:
            bv.doubleBinding = mm.readDouble();
            break;
        case Fields.FIELD_TYPE_TIME:
            bv.value = mm.readTime();
            break;
        case Fields.FIELD_TYPE_DATE:
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP:
            bv.value = mm.readDate();
            break;
        case Fields.FIELD_TYPE_VAR_STRING:
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR:
            bv.value = mm.readStringWithLength(charset);
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL:
            bv.value = mm.readBigDecimal();
            if (bv.value == null) {
                bv.isNull = true;
            }
            break;
        default:
            throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.type);
        }
        bv.isSet = true;
    }

}