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
package org.opencloudb.net.postgres;

/**
 * <pre>
 * NoticeResponse (B) 
 * Byte1('N') Identifies the message as a notice. 
 * Int32 Length of message contents in bytes, including self. The message 
 *       body consists of one or more identified fields, followed by a zero 
 *       byte as a terminator. Fields can appear in any order. For each 
 *       field there is the following: 
 * Byte1 A code identifying the field type; if zero, this is the message 
 *       terminator and no string follows. The presently defined field types 
 *       are listed in Section 46.6. Since more field types might be added
 *       in future, frontends should silently ignore fields of unrecognized 
 *       type.
 * String The field value.
 * </pre>
 * 
 * @author mycat
 */
public class NoticeResponse extends PostgresPacket {

}