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
 * Bind (F) 
 * Byte1('B') Identifies the message as a Bind command. 
 * Int32 Length of message contents in bytes, including self. 
 * String The name of the destination portal (an empty string selects the unnamed portal).
 * String The name of the source prepared statement (an empty string selects the unnamed 
 *        prepared statement). 
 * Int16 The number of parameter format codes that follow (denoted C below). 
 *       This can be zero to indicate that there are no parameters or that the parameters 
 *       all use the default format(text); or one, in which case the specified format code 
 *       is applied to all parameters; or it can equal the actual number of parameters. 
 * Int16[C] The parameter format codes. Each must presently be zero (text) or one(binary). 
 * Int16 The number of parameter values that follow (possibly zero). This must match the 
 *       number of parameters needed by the query. Next, the following pair of fields appear 
 *       for each parameter: 
 * Int32 The length of the parameter value, in bytes (this count does not include
 *       itself). Can be zero. As a special case, -1 indicates a NULL parameter
 *       value. No value bytes follow in the NULL case. 
 * Byten The value of the parameter, in the format indicated by the associated format code. 
 *       n is the above length. After the last parameter, the following fields appear:
 * Int16 The number of result-column format codes that follow (denoted R
 *       below). This can be zero to indicate that there are no result columns or
 *       that the result columns should all use the default format (text); or one,
 *       in which case the specified format code is applied to all result columns
 *       (if any); or it can equal the actual number of result columns of the query. 
 * Int16[R] The result-column format codes. Each must presently be zero (text) or one (binary).
 * </pre>
 * 
 * @author mycat
 */
public class Bind extends PostgresPacket {

}