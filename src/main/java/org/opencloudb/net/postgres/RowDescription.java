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
 * RowDescription (B) 
 * Byte1('T') Identifies the message as a row description. 
 * Int32 Length of message contents in bytes, including self.
 * Int16 Specifies the number of fields in a row (can be zero). Then, for
 *       each field,there is the following: String The field name. 
 * Int32 If the field can be identified as a column of a specific table, 
 *       the object ID of the table; otherwise zero. 
 * Int16 If the field can be identified as a column of a specific table, the 
 *       attribute number of the column; otherwise zero. 
 * Int32 The object ID of the field's data type. 
 * Int16 The data type size (see pg_type.typlen). Note that negative values 
 *       denote variable-width types. 
 * Int32 The type modifier (see pg_attribute.atttypmod). The meaning of the 
 *       modifier is type-specific.
 * Int16 The format code being used for the field. Currently will be zero
 *       (text) or one (binary). In a RowDescription returned from the 
 *       statement variant of Describe, the format code is not yet known and 
 *       will always be zero.
 * </pre>
 * 
 * @author mycat
 */
public class RowDescription extends PostgresPacket {

}