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
 * CommandComplete (B)
 * Byte1('C') Identifies the message as a command-completed response.     
 * Int32 Length of message contents in bytes, including self.     
 * String The command tag. This is usually a single word that identifies which SQL command was completed. 
 *        For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. 
 *        oid is the object ID of the inserted row if rows is 1 and the target table has OIDs; otherwise oid is 0.  
 *        For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted. 
 *        For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated. 
 *        For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved. 
 *        For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by. 
 *        For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor. 
 *        For a COPY command, the tag is COPY rows where rows is the number of rows copied. 
 *        (Note: the row count appears only in PostgreSQL 8.2 and later.)
 * </pre>
 * 
 * @author mycat
 */
public class CommandComplete extends PostgresPacket {

}