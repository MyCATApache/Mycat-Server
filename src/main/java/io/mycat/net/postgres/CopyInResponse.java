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
package io.mycat.net.postgres;

/**
 * <pre>
 * CopyInResponse (B)     
 * Byte1('G') Identifies the message as a Start Copy In response. 
 *            The frontend must now send copy-in data (if not prepared 
 *            to do so, send a CopyFail message).
 * Int32 Length of message contents in bytes, including self.
 * Int8 0 indicates the overall COPY format is textual (rows separated 
 *      by newlines, columns separated by separator characters, etc). 
 *      1 indicates the overall copy format is binary (similar to DataRow 
 *      format). See COPY for more information.
 * Int16 The number of columns in the data to be copied (denoted N below).
 * Int16[N] The format codes to be used for each column. Each must presently 
 *          be zero (text) or one (binary). All must be zero if the overall 
 *          copy format is textual.
 * </pre>
 * 
 * @author mycat
 */
public class CopyInResponse extends PostgresPacket {

}