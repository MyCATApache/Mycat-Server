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
package io.mycat.mysql;

import org.junit.Assert;
import org.junit.Test;

import io.mycat.backend.mysql.MySQLMessage;

/**
 * @author mycat
 */
public class MySQLMessageTest {

    @Test
    public void testReadBytesWithNull() {
        byte[] bytes = new byte[] { 1, 2, 3, 0, 5 };
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(3, ab.length);
        Assert.assertEquals(4, message.position());
    }

    @Test
    public void testReadBytesWithNull2() {
        byte[] bytes = new byte[] { 0, 1, 2, 3, 0, 5 };
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(1, message.position());
    }

    @Test
    public void testReadBytesWithNull3() {
        byte[] bytes = new byte[] {};
        MySQLMessage message = new MySQLMessage(bytes);
        byte[] ab = message.readBytesWithNull();
        Assert.assertEquals(0, ab.length);
        Assert.assertEquals(0, message.position());
    }

}