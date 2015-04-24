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
 * ReadyForQuery (B) 
 * Byte1('Z') Identifies the message type. ReadyForQuery is sent whenever the 
 *            backend is ready for a new query cycle. 
 * Int32(5) Length of message contents in bytes, including self. 
 * Byte1 Current backend transaction status indicator. Possible values are 'I' 
 *       if idle(not in a transaction block); 'T' if in a transaction block; 
 *       or 'E' if in a failed transaction block (queries will be rejected until
 *       block is ended).
 * </pre>
 * 
 * @author mycat
 */
public class ReadyForQuery extends PostgresPacket {

}