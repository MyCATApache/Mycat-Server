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
 * Parse (F) 
 * Byte1('P') Identifies the message as a Parse command. 
 * Int32 Length of message contents in bytes, including self. 
 * String The name of the destination prepared statement (an empty string 
 *        selects the unnamed prepared statement). 
 * String The query string to be parsed. 
 * Int16 The number of parameter data types specified (can be zero). Note 
 *       that this is not an indication of the number of parameters that 
 *       might appear in the query string, only the number that the frontend 
 *       wants to prespecify types for. Then, for each parameter, there is 
 *       the following: 
 * Int32 Specifies the object ID of the parameter data type. Placing a zero 
 *       here is equivalent to leaving the type unspecified.
 * </pre>
 * 
 * @author mycat
 */
public class Parse extends PostgresPacket {

}