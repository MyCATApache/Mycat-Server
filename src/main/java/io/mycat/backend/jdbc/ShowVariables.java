/*
 * Copyright (c) 2020, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
package io.mycat.backend.jdbc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.*;
import io.mycat.backend.mysql.listener.SqlExecuteStage;
import io.mycat.net.mysql.*;
import io.mycat.route.parser.druid.MycatStatementParser;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

/**
 * @author mycat
 */
public final class ShowVariables
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowVariables.class);
    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    private static final    Pattern pattern = Pattern.compile("(?:like|=)\\s*'([^']*(?:\\w+)+[^']*)+'",Pattern.CASE_INSENSITIVE);
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("VARIABLE_NAME", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }
    private static List<String> parseVariable(String sql)
    {
        List<String> variableList=new ArrayList<>();
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find())
        {
            variableList.add(matcher.group(1));
        }
        return variableList;
    }
    public static void execute(ServerConnection c, String sql) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;

        List<String> variableList= parseVariable(sql);
        for (String key : variableList)
        {
          String value=  variables.get(key)  ;
            if(value!=null)
            {
                RowDataPacket row = getRow(key, value, c.getCharset());
                row.packetId = ++packetId;
                buffer = row.write(buffer, c,true);
            }
        }



        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    public static void justReturnValue(ServerConnection c, String value) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;



            if(value!=null)
            {

                RowDataPacket row = new RowDataPacket(1);
                row.add(StringUtil.encode(value, c.getCharset()));
                row.packetId = ++packetId;
                buffer = row.write(buffer, c,true);
            }



        // write lastEof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String name, String value, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(name, charset));
        row.add(StringUtil.encode(value, charset));
        return row;
    }

    private static final Map<String, String> variables = new HashMap<String, String>();
    static {
        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "16384");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");

        //add by =
        variables.put("auto_increment_increment", "1");
    }
    
     public static void execute(ServerConnection sc, String orgin, BackendConnection jdbcConnection) {
        execute(sc, orgin);
        NonBlockingSession session = sc.getSession2();
        session.releaseConnectionIfSafe(jdbcConnection, LOGGER.isDebugEnabled(), false);
        session.getSource().getListener().fireEvent(SqlExecuteStage.END);
    }
     public static void justReturnValue(ServerConnection sc, String orgin, BackendConnection jdbcConnection) {
    	 justReturnValue(sc, orgin);
         NonBlockingSession session = sc.getSession2();
         session.releaseConnectionIfSafe(jdbcConnection, LOGGER.isDebugEnabled(), false);
         session.getSource().getListener().fireEvent(SqlExecuteStage.END);
     }

    public static void executeSelectVar(ServerConnection c, String sql, BackendConnection jdbcConnection) throws UnExecutedException {

        ResultSetHeaderPacket header;
        FieldPacket[] fields;
        EOFPacket eof;
        RowDataPacket row;
        EOFPacket lastEof;

        try {
            MycatStatementParser parser = new MycatStatementParser(sql);
            SQLSelectStatement sss = parser.parseSelect();
            SQLSelect ss = sss.getSelect();
            SQLSelectQueryBlock qry = ss.getQueryBlock();
            if (null != qry.getFrom() && !"dual".equalsIgnoreCase(qry.getFrom().toString())) {
                throw new UnExecutedException("format error");
            }

            List<SQLSelectItem> ssis = qry.getSelectList();
            int FIELD_COUNT = ssis.size();

            byte packetId = 0;

            header = PacketUtil.getHeader(FIELD_COUNT);
            header.packetId = ++packetId;

            fields = new FieldPacket[FIELD_COUNT];
            for(int i = 0; i < FIELD_COUNT; i++) {
                SQLSelectItem ssi = ssis.get(i);
                if (null == ssi.getAlias() || null == ssi.getExpr() || !ssi.getExpr().toString().startsWith("@@")) {
                    throw new UnExecutedException("format error");
                }
                fields[i] = PacketUtil.getField(ssi.getAlias(), Fields.FIELD_TYPE_VAR_STRING);
                fields[i].packetId = ++packetId;
            }

            eof = new EOFPacket();
            eof.packetId = ++packetId;

            row = new RowDataPacket(FIELD_COUNT);
            for (SQLSelectItem ssi: ssis) {
                String val = variables.get(ssi.getAlias());
                row.add(StringUtil.encode(val, c.getCharset()));
            }
            row.packetId = ++packetId;

            // write lastEof
            lastEof = new EOFPacket();
            lastEof.packetId = ++packetId;

        } catch (Throwable e) {
            throw new UnExecutedException(e);
        }

        ByteBuffer buffer = c.allocate();
        // write header
        buffer = header.write(buffer, c,true);
        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }
        // write eof
        buffer = eof.write(buffer, c,true);
        buffer = row.write(buffer, c,true);
        buffer = lastEof.write(buffer, c,true);
        // write buffer
        c.write(buffer);

        NonBlockingSession session = c.getSession2();
        session.releaseConnectionIfSafe(jdbcConnection, LOGGER.isDebugEnabled(), false);
        session.getSource().getListener().fireEvent(SqlExecuteStage.END);
    }

    public static void executeSetVar(ServerConnection c, String sql, BackendConnection jdbcConnection) throws UnExecutedException {

        try {
            MycatStatementParser parser = new MycatStatementParser(sql);
            SQLSetStatement ss = (SQLSetStatement)parser.parseSet();
            for (SQLAssignItem item : ss.getItems()) {
                String tagert = ((SQLVariantRefExpr)item.getTarget()).getName();
                String value = null;
                if (item.getValue() instanceof SQLCharExpr){
                    value = ((SQLCharExpr) item.getValue()).getText();
                } else if (item.getValue() instanceof SQLIntegerExpr) {
                    value = ((SQLIntegerExpr) item.getValue()).getNumber().toString();
                }
                if (tagert.startsWith("@@") && null != value) {
                    tagert = tagert.substring(2);
                    variables.put(tagert, value);
                } else {
                    throw new UnExecutedException("format error");
                }
            }
        } catch (Throwable e) {
            throw new UnExecutedException(e);
        }

        c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));

        NonBlockingSession session = c.getSession2();
        session.releaseConnectionIfSafe(jdbcConnection, LOGGER.isDebugEnabled(), false);
        session.getSource().getListener().fireEvent(SqlExecuteStage.END);
    }
}