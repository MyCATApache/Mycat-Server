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
import com.google.common.io.Files;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.mpp.LoadData;
import org.opencloudb.net.handler.LoadDataInfileHandler;
import org.opencloudb.net.mysql.BinaryPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RequestFilePacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.ObjectUtil;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.*;

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

    private Map<String,LoadData>  routeResultMap=new HashMap<>();

    private LoadData loadData;
    private ByteBuffer tempByteBuffer;
    private long tempByteBuffrSize=0;
    private String tempFile;
    private boolean isHasStoreToFile=false;
    private String tempPath;

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






    private  void parseLoadDataPram()
    {
        loadData=new LoadData();
        SQLLiteralExpr rawLineEnd = statement.getLinesTerminatedBy();
        String lineTerminatedBy = rawLineEnd == null ? "\n" : rawLineEnd.toString();
        loadData.setLineTerminatedBy(lineTerminatedBy);

        SQLLiteralExpr rawFieldEnd = statement.getColumnsTerminatedBy();
        String fieldTerminatedBy = rawFieldEnd == null ? "\t" : rawFieldEnd.toString();
        loadData.setFieldTerminatedBy(fieldTerminatedBy);

        SQLLiteralExpr rawEnclosed = statement.getColumnsEnclosedBy();
        String enclose = rawEnclosed == null ? "" : rawEnclosed.toString();
        loadData.setEnclose(enclose);

        String charset = statement.getCharset() != null ? statement.getCharset() : serverConnection.getCharset();
        loadData.setCharset(charset);
        loadData.setFileName(fileName);
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
        tempPath=SystemConfig.getHomePath()+File.pathSeparator+"temp"+File.pathSeparator+serverConnection.getId()+File.pathSeparator;
        tempByteBuffer=serverConnection.allocate();
        parseLoadDataPram();
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
                RouteResultset rrs =     buildResultSet(routeResultMap);
                if(rrs!=null)
                {
                    serverConnection.getSession2().execute(rrs,ServerParse.LOAD_DATA_INFILE_SQL);
                }
             //   sendOk((byte) 1);
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

             saveByteOrToFile(packet.data,false);




        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }


    }

    private synchronized void saveByteOrToFile(byte[] data,boolean isForce)
    {

        if(data!=null)
        {
            tempByteBuffrSize=tempByteBuffrSize+data.length;
            tempByteBuffer.put(data);
        }

         if((isForce&&isHasStoreToFile)||tempByteBuffrSize>200*1024*1024)    //超过200M 存文件
        {
            FileChannel channel = null;
            try
            {
                channel = new FileOutputStream(new File(tempFile), true).getChannel();
                tempByteBuffer.flip();
                channel.write(tempByteBuffer);
                tempByteBuffrSize=0;
                isHasStoreToFile=false;
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            } finally
            {
                try
                {
                    if(channel!=null)
                    channel.close();
                } catch (IOException ignored)
                {

                }
            }


        }
    }

    private void parseOneLine(List<SQLExpr> columns, String tableName, String line,boolean toFile)
    {
        List<String> fields = Splitter.on(loadData.getFieldTerminatedBy()).splitToList(line);
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
              LoadData data= routeResultMap.get(name) ;
             if(data==null)
               {
                   routeResultMap.put(name,new LoadData());
               }
                if(toFile)
                {
                    if(data.getFileName()==null)
                    {

                    }   else
                    {

                    }

                }   else
                {
                    if (data.getData() == null)
                    {
                        data.setData(Lists.newArrayList(line));
                    } else
                    {
                        data.getData().add(line);
                    }
                }
            }
        }
    }




    private RouteResultset buildResultSet(Map<String,List<String>>  routeMap)
    {
        statement.setLocal(true);//强制local
        SQLLiteralExpr fn=new SQLCharExpr(fileName);    //默认druid会过滤掉路径的分隔符，所以这里重新设置下
        statement.setFileName(fn);
        String srcStatement = statement.toString();
        RouteResultset rrs=new RouteResultset(srcStatement,ServerParse.LOAD_DATA_INFILE_SQL);
        rrs.setStatement(srcStatement);
        rrs.setAutocommit(serverConnection.isAutocommit());
        rrs.setFinishedRoute(true);
        int size = routeMap.size();
        RouteResultsetNode[] routeResultsetNodes=new RouteResultsetNode[size];
        int index=0;
        for (String dn : routeMap.keySet())
        {
            RouteResultsetNode rrNode = new RouteResultsetNode(dn, ServerParse.LOAD_DATA_INFILE_SQL, srcStatement);
            rrNode.setTotalNodeSize(size);
            rrNode.setStatement(srcStatement);
            LoadData newLoadData=new LoadData();
            ObjectUtil.copyProperties(loadData,newLoadData);
            newLoadData.setLocal(true);
            newLoadData.setData(routeMap.get(dn));
            rrNode.setLoadData(newLoadData);

            routeResultsetNodes[index]=rrNode;
            index++;
        }
         rrs.setNodes(routeResultsetNodes);
       return rrs;
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

        //load in data空包 结束
        saveByteOrToFile(null,true);
        List<SQLExpr> columns = statement.getColumns();
        String tableName = statement.getTableName().getSimpleName();
         if(isHasStoreToFile)
         {

         }   else
         {
         String content=byteBufferToString(tempByteBuffer,loadData.getCharset());
        List<String> lines = Splitter.on(loadData.getLineTerminatedBy()).omitEmptyStrings().splitToList(content);
        for (final String line : lines)
        {
            parseOneLine(columns, tableName, line);


        }

        RouteResultset rrs =     buildResultSet(routeResultMap);
        if(rrs!=null)
        {
           serverConnection.getSession2().execute(rrs,ServerParse.LOAD_DATA_INFILE_SQL);
        }
         }

       // sendOk(++packID);
          clear();

    }


    private static void parseFileByLine(String file,String encode,String split)
    {
        Scanner scanner =null;
        try {
            scanner = new Scanner(new FileInputStream(file), encode);
            scanner.useDelimiter(split);
            while (scanner.hasNextLine()){

            }
        } catch (FileNotFoundException e)
        {
          throw new RuntimeException(e);
        } finally{
            if(scanner!=null)
            scanner.close();
        }

    }


    private static String byteBufferToString(ByteBuffer buffer,String charsetIn) {
        CharBuffer charBuffer = null;
        try {
            Charset charset = Charset.forName(charsetIn);
            CharsetDecoder decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer);
            buffer.flip();
            return charBuffer.toString();
        } catch (Exception ex) {
            return null;
        }
    }

//    private void sendOk(byte packID)
//    {
//        OkPacket ok = new OkPacket();
//        ok.packetId = packID;
//        ok.affectedRows = affectedRows;
//
//        ok.serverStatus = serverConnection.isAutocommit() ? 2 : 1;
//        String msg = "Records:" + affectedRows + " Deleted:0 Skipped:0 Warnings:0";
//        try
//        {
//            ok.message = msg.getBytes("utf-8");
//        } catch (UnsupportedEncodingException e)
//        {
//            throw new RuntimeException(e);
//        }
//        ok.write(serverConnection);
//    }


    public void clear()
    {
        File temp=new File(tempFile);
        if(temp.exists())
        {
            temp.delete();
        }
        if(new File(tempPath).exists())
        {
            Files.
        }
        tempByteBuffer=null;
        loadData=null;
        sql = null;
        fileName = null;
        statement = null;
        routeResultMap.clear();
    }
}