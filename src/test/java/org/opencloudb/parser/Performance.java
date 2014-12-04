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
package org.opencloudb.parser;

/**
 * @author mycat
 */
public interface Performance {
    String SQL_BENCHMARK_SELECT = " seLEcT id, member_id , image_path  \t , image_size , STATUS,   gmt_modified from    wp_image wheRe \t\t\n id =  ? AND member_id\t=\t-123.456";
    // String SQL_BENCHMARK_SELECT =
    // "select ID, GMT_CREATE, GMT_MODIFIED, INBOX_FOLDER_ID, MESSAGE_ID,             FEEDBACK_TYPE, TARGET_ID,               TRADE_ID, SUBJECT, SENDER_ID, SENDER_TYPE,              S_DISPLAY_NAME, SENDER_STATUS, RECEIVER_ID, RECEIVER_TYPE,              R_DISPLAY_NAME, RECEIVER_STATUS, SPAM_STATUS, REPLY_STATUS,             ATTACHMENT_STATUS,              SENDER_COUNTRY,                 RECEIVER_COUNTRY,APP_FROM,APP_TO,APP_SOURCE,SENDER_VACOUNT,RECEIVER_VACOUNT,            DISTRIBUTE_STATUS,ORG_RECEIVER_ID,CUSTOMER_ID,OPERATOR_ID,OPERATOR_NAME,FOLLOW_STATUS,DELETE_STATUS,FOLLOW_TIME,BATCH_COUNT             from MESSAGE_REC_RECORD                 where RECEIVER_VACOUNT          =? and ID = ?";
    String SQL_BENCHMARK_EXPR_SELECT = "( seLect id, member_id , image_path  \t , image_size , STATUS,   gmt_modified from    wp_image where \t\t\n id =  ? and member_id\t=\t?)";
}