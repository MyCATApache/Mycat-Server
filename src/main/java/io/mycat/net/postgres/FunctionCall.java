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
 * FunctionCall (F) 
 * Byte1('F') Identifies the message as a function call.
 * Int32 Length of message contents in bytes, including self. 
 * Int32 Specifies the object ID of the function to call. 
 * Int16 The number of argument format codes that follow (denoted C below). 
 *       This can be zero to indicate that there are no arguments or that 
 *       the arguments all use the default format (text); or one, in which 
 *       case the specified format code is applied to all arguments; or it 
 *       can equal the actual number of arguments.
 * Int16[C] The argument format codes. Each must presently be zero (text) or
 *          one (binary). 
 * Int16 Specifies the number of arguments being supplied to the function. 
 *       Next, the following pair of fields appear for each argument: 
 * Int32 The length of the argument value, in bytes (this count does not include 
 *       itself). Can be zero. As a special case, -1 indicates a NULL argument 
 *       value. No value bytes follow in the NULL case. 
 * Byten The value of the argument, in the format indicated by the associated 
 *       format code. n is the above length. After the last argument, the 
 *       following field appears: 
 * Int16 The format code for the function result. Must presently be zero (text) 
 *       or one (binary).
 * </pre>
 * 
 * @author mycat
 */
public class FunctionCall extends PostgresPacket {

}