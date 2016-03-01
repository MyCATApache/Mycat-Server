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
public class BindValue {

    public boolean isNull; /* NULL indicator */
    public boolean isLongData; /* long data indicator */
    public boolean isSet; /* has this parameter been set */

    public long length; /* Default length of data */
    public int type; /* data type */
    public byte scale;

    /** 数据值 **/
    public byte byteBinding;
    public short shortBinding;
    public int intBinding;
    public float floatBinding;
    public long longBinding;
    public double doubleBinding;
    public Object value; /* Other value to store */

    public void reset() {
        this.isNull = false;
        this.isLongData = false;
        this.isSet = false;

        this.length = 0;
        this.type = 0;
        this.scale = 0;

        this.byteBinding = 0;
        this.shortBinding = 0;
        this.intBinding = 0;
        this.floatBinding = 0;
        this.longBinding = 0L;
        this.doubleBinding = 0D;
        this.value = null;
    }

}