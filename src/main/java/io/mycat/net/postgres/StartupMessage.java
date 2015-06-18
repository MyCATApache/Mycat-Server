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
 * StartupMessage (F) 
 * Int32 Length of message contents in bytes, including self. 
 * Int32(196608) The protocol version number. The most significant 16 bits are 
 *               the major version number (3 for the protocol described here).
 *               The least significant 16 bits are the minor version number (0 
 *               for the protocol described here). The protocol version number 
 *               is followed by one or more pairs of parameter name and value 
 *               strings. A zero byte is required as a terminator after the 
 *               last name/value pair. Parameters can appear in any order. user 
 *               is required, others are optional. Each parameter is specified as: 
 * String The parameter name. Currently recognized names are: 
 *        user The database user name to connect as. Required; there is no default. 
 *        database The database to connect to. Defaults to the user name. 
 *        options Command-line arguments for the backend. (This is deprecated in 
 *                favor of setting individual run-time parameters.) In addition to
 *                the above, any run-time parameter that can be set at backend start 
 *                time might be listed. Such settings will be applied during backend 
 *                start (after parsing the command-line options if any). The values 
 *                will act as session defaults. 
 * String The parameter value.
 * </pre>
 * 
 * @author mycat
 */
public class StartupMessage extends PostgresPacket {

}