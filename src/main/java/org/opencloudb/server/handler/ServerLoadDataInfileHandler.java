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
package org.opencloudb.server.handler;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.net.handler.LoadDataInfileHandler;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * mysql命令行客户端也需要启用local file权限，加参数--local-infile=1
 * jdbc则正常，不用设置
 * load data sql中的CHARACTER SET 'gbk'   其中的字符集必须引号括起来，否则druid解析出错
 */
public final class ServerLoadDataInfileHandler implements LoadDataInfileHandler
{
    private ServerConnection serverConnection;
    private String sql;
    private String fileName;
    private long affectedRows;
    private MySqlLoadDataInFileStatement statement;

    private Map<String,List<String>>  routeResultMap=new HashMap<>();


    public ServerLoadDataInfileHandler(ServerConnection serverConnection)
    {
        this.serverConnection = serverConnection;

    }

    private static String parseFileName(String sql)
    {
        if (sql.contains("'"))
        {
            int beginIndex = sql.indexOf("'");
            return sql.substring(beginIndex + 1, sql.indexOf("'", beginIndex + 1));
        } else if (sql.contains("\""))
        {
            int beginIndex = sql.indexOf("\"");
            return sql.substring(beginIndex + 1, sql.indexOf("\"", beginIndex + 1));
        }
        return null;
    }


    @Override
    public void start(String sql)
    {
        this.sql = sql;


        SQLStatementParser parser = new MySqlStatementParser(sql);
        statement = (MySqlLoadDataInFileStatement) parser.parseStatement();
        fileName = parseFileName(sql);
        if (fileName == null)
        {
            serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, " file name is null !");
            clear();
            return;
        }
        if (statement.isLocal())
        {
            //向客户端请求发送文件
            ByteBuffer buffer = serverConnection.allocate();
            RequestFilePacket filePacket = new RequestFilePacket();
            filePacket.fileName = fileName.getBytes();
            filePacket.packetId = 1;
            filePacket.write(buffer, serverConnection, true);
        } else
        {
            if (!new File(fileName).exists())
            {
                serverConnection.writeErrMessage(ErrorCode.ER_FILE_NOT_FOUND, fileName + " is not found!");
                clear();
            } else
            {
                sendOk((byte) 1);
                clear();
            }
        }
    }

    @Override
    public void handle(byte[] data)
    {

        try
        {
            BinaryPacket packet = new BinaryPacket();
            ByteInputStream inputStream = new ByteInputStream(data, data.length);
            packet.read(inputStream);

            SQLLiteralExpr rawLineEnd = statement.getLinesTerminatedBy();
            String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.toString();

            SQLLiteralExpr rawFieldEnd = statement.getColumnsTerminatedBy();
            String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.toString();

            SQLLiteralExpr rawEnclosed = statement.getColumnsEnclosedBy();
            String enclose = rawEnclosed == null ? "" : rawEnclosed.toString();

            String charset = statement.getCharset() != null ? statement.getCharset() : "utf-8";

            List<SQLExpr> columns = statement.getColumns();
            String tableName = statement.getTableName().getSimpleName();

            String content = new String(data, 4, data.length - 4, charset);
            List<String> lines = Splitter.on(lineTerminatedBy).omitEmptyStrings().splitToList(content);
            for (final String line : lines)
            {
                List<String> fields = Splitter.on(fieldTerminatedBy).splitToList(line);
                String insertSql = makeSimpleInsert(columns, fields, tableName, true);
                RouteResultset rrs = serverConnection.routeSQL(insertSql, ServerParse.INSERT);


                //验证正确性，之后删除
                String insertSql1 = makeSimpleInsert(columns, fields, tableName, false);
                RouteResultset rrs1 = serverConnection.routeSQL(insertSql1, ServerParse.INSERT);
                if (!rrs.getNodes()[0].getName().equals(rrs1.getNodes()[0].getName()))
                {
                    throw new RuntimeException("路由错误");
                }

                if(rrs==null ||rrs.getNodes()==null||rrs.getNodes().length==0)
                {
                    //无路由处理
                }
                else
                {
                    for (RouteResultsetNode routeResultsetNode : rrs.getNodes())
                    {
                      String name=  routeResultsetNode.getName();
                     List<String>   linesData=     routeResultMap.get(name);
                        if(linesData==null)
                        {
                            routeResultMap.put(name, Lists.newArrayList(line)) ;
                        }  else
                        {
                            linesData.add(line);
                        }
                    }
                }


            }


            //   Files.write(packet.data, new File("d:\\88\\mycat.txt"));

        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }


    }



    private void buildResultSet()
    {
        statement.setLocal(true);//强制local
        SQLLiteralExpr fn=new SQLCharExpr(fileName);    //默认druid会过滤掉路径的分隔符，所以这里重新设置下
        statement.setFileName(fn);
        RouteResultset rrs=new RouteResultset(statement.toString(),ServerParse.LOAD_DATA_INFILE_SQL);
        rrs.setAutocommit(serverConnection.isAutocommit());



    }




    private String makeSimpleInsert(List<SQLExpr> columns, List<String> fields, String table, boolean isAddEncose)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into ").append(table.toUpperCase());
        if (columns != null && columns.size() > 0)
        {
            sb.append("(");
            for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++)
            {
                SQLExpr column = columns.get(i);
                sb.append(column.toString());
                if (i != columnsSize - 1)
                {
                    sb.append(",");
                }
            }
            sb.append(") ");
        }

        sb.append(" values (");
        for (int i = 0, columnsSize = fields.size(); i < columnsSize; i++)
        {
            String column = fields.get(i);
            if (isAddEncose)
            {
                sb.append("'").append(column).append("'");
            } else
            {
                sb.append(column);
            }
            if (i != columnsSize - 1)
            {
                sb.append(",");
            }
        }
        sb.append(")");
        return sb.toString();
    }


    @Override
    public void end(byte packID)
    {
        affectedRows = 10;
        //load in data空包 结束
        sendOk(++packID);
          clear();

    }

    private void sendOk(byte packID)
    {
        OkPacket ok = new OkPacket();
        ok.packetId = packID;
        ok.affectedRows = affectedRows;

        ok.serverStatus = serverConnection.isAutocommit() ? 2 : 1;
        String msg = "Records:" + affectedRows + " Deleted:0 Skipped:0 Warnings:0";
        try
        {
            ok.message = msg.getBytes("utf-8");
        } catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
        ok.write(serverConnection);
    }


    public void clear()
    {
        sql = null;
        fileName = null;
        statement = null;
        routeResultMap.clear();
    }
}