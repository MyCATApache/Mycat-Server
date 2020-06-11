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
package io.mycat.server;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.defCommand.DefaultSqlCommandInterceptor;
import io.mycat.defCommand.MycatRowMetaData;
import io.mycat.defCommand.Response;
import io.mycat.defCommand.RowBaseIterator;
import io.mycat.net.handler.FrontendQueryHandler;
import io.mycat.net.mysql.*;
import io.mycat.route.RouteService;
import io.mycat.server.handler.*;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.MysqlDefs;
import io.mycat.util.ResultSetUtil;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Supplier;

/**
 * @author mycat
 */
public class ServerQueryHandler implements FrontendQueryHandler {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(ServerQueryHandler.class);

    private final ServerConnection source;
    protected Boolean readOnly;

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public ServerQueryHandler(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void query(String sql) {

        ServerConnection c = this.source;

        DefaultSqlCommandInterceptor defaultSqlCommandInterceptor = MycatServer.getInstance().getDefaultSqlCommandInterceptor();
        if (defaultSqlCommandInterceptor != null) {
			Response response = new Response() {

				@Override
				public void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator) {
					RowBaseIterator rowBaseIterator1 = null;
                    ByteBuffer byteBuf = source.allocate();
					try {
						rowBaseIterator1 = Objects.requireNonNull(rowBaseIterator.get());
						byte packetId = 0;
						List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
						ResultSetUtil.resultSetToFieldPacket(source.getCharset(), fieldPks, rowBaseIterator1.getMetaData(),
								false);
						int colunmCount = fieldPks.size();
						ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
						headerPkg.fieldCount = fieldPks.size();
						headerPkg.packetId = ++packetId;

						byteBuf = headerPkg.write(byteBuf, source, true);
						byteBuf.flip();
						byte[] header = new byte[byteBuf.limit()];
						byteBuf.get(header);
						source.write(header);
						byteBuf.clear();
						List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
						Iterator<FieldPacket> itor = fieldPks.iterator();
						while (itor.hasNext()) {
							FieldPacket curField = itor.next();
							curField.packetId = ++packetId;
							byteBuf = curField.write(byteBuf, source, false);
							byteBuf.flip();
							byte[] field = new byte[byteBuf.limit()];
							byteBuf.get(field);
                            source.write(field);
							byteBuf.clear();
							fields.add(field);
						}

						EOFPacket eofPckg = new EOFPacket();
						eofPckg.packetId = ++packetId;
						byteBuf = eofPckg.write(byteBuf, source, false);
						byteBuf.flip();
						byte[] eof = new byte[byteBuf.limit()];
						byteBuf.get(eof);
                        source.write(eof);
						byteBuf.clear();

						// output row
						TextResultSetResponse textResultSetResponse = new TextResultSetResponse(rowBaseIterator1);
						Iterator<byte[]> iterator = textResultSetResponse.rowIterator();
						while (iterator.hasNext()) {
                            byte[] next = iterator.next();
                            RowDataPacket curRow = new RowDataPacket(colunmCount);
							for (byte[] bytes : textResultSetResponse.row) {
								curRow.add(bytes);
							}
							curRow.packetId = ++packetId;
							byteBuf = curRow.write(byteBuf, source, false);
							byteBuf.flip();
							byte[] row = new byte[byteBuf.limit()];
							byteBuf.get(row);
							byteBuf.clear();
							source.write(row);
						}

						// end row
						eofPckg = new EOFPacket();
						eofPckg.packetId = ++packetId;
						byteBuf = eofPckg.write(byteBuf, source, false);
						byteBuf.flip();
						eof = new byte[byteBuf.limit()];
						byteBuf.get(eof);
						source.write(eof);

					} finally {
					    if (byteBuf!=null) {
                            source.recycle(byteBuf);
                        }
						if (rowBaseIterator1 != null) {
							rowBaseIterator1.close();
						}
					}
				}

				@Override
				public void sendError(String t) {
					source.writeErrMessage(ErrorCode.ER_YES,t);
				}
			};

			if (defaultSqlCommandInterceptor.match(c, sql,response)) {
                if (defaultSqlCommandInterceptor.handle(c, sql,response)) {
                    return;
                }
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(c).append(sql).toString());
        }
        //
        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;

        switch (sqlType) {
            //explain sql
            case ServerParse.EXPLAIN:
                ExplainHandler.handle(sql, c, rs >>> 8);
                break;
            //explain2 datanode=? sql=?
            case ServerParse.EXPLAIN2:
                Explain2Handler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.COMMAND:
                CommandHandler.handle(sql, c, 16);
                break;
            case ServerParse.SET:
                SetHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SHOW:
                ShowHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.SELECT:
                SelectHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.START:
                StartHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.BEGIN:
                BeginHandler.handle(sql, c);
                break;
            //不支持oracle的savepoint事务回退点
            case ServerParse.SAVEPOINT:
                SavepointHandler.handle(sql, c);
                break;
            case ServerParse.KILL:
                KillHandler.handle(sql, rs >>> 8, c);
                break;
            //不支持KILL_Query
            case ServerParse.KILL_QUERY:
                LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
                c.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
                break;
            case ServerParse.USE:
                UseHandler.handle(sql, c, rs >>> 8);
                break;
            case ServerParse.COMMIT:
                c.commit();
                break;
            case ServerParse.ROLLBACK:
                c.rollback();
                break;
            case ServerParse.HELP:
                LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
                c.writeErrMessage(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");
                break;
            case ServerParse.MYSQL_CMD_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.MYSQL_COMMENT:
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
                break;
            case ServerParse.LOAD_DATA_INFILE_SQL:
                if (RouteService.isHintSql(sql) > -1) { // 目前仅支持注解 datanode,原理为直接将导入sql发送到指定mysql节点
                    c.execute(sql, ServerParse.LOAD_DATA_INFILE_SQL);
                } else {
                    c.loadDataInfileStart(sql);
                }
                break;
            case ServerParse.MIGRATE: {
                try {
                    MigrateHandler.handle(sql, c);
                } catch (Throwable e) {
                    //MigrateHandler中InterProcessMutex slaveIDsLock 会连接zk,zk连接不上会导致类加载失败,
                    // 此后再调用此命令,将会出现类未定义,所以最终还是需要重启mycat
                    e.printStackTrace();
                    String msg = "Mycat is not connected to zookeeper!!\n";
                    msg += "Please start zookeeper and restart mycat so that this mycat can temporarily execute the migration command.If other mycat does not connect to this zookeeper, they will not be able to perceive changes in the migration task.\n";
                    msg += "After starting zookeeper,you can command tas follow:\n\nmigrate -table=schema.test -add=dn2,dn3 -force=true\n\nto perform the migration.\n";
                    LOGGER.error(e.getMessage());
                    LOGGER.error(msg);
                    c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
                }
                break;
            }
            case ServerParse.LOCK:
                c.lockTable(sql);
                break;
            case ServerParse.UNLOCK:
                c.unLockTable(sql);
                break;
            default:
                if (readOnly) {
                    LOGGER.warn(new StringBuilder().append("User readonly:").append(sql).toString());
                    c.writeErrMessage(ErrorCode.ER_USER_READ_ONLY, "User readonly");
                    break;
                }
                c.execute(sql, rs & 0xff);
        }

        switch (sqlType) {
            case ServerParse.SELECT:
            case ServerParse.DELETE:
            case ServerParse.UPDATE:
            case ServerParse.INSERT:
            case ServerParse.COMMAND:
                // curd 在后面会更新
                break;
            default:
                c.setExecuteSql(null);
        }
    }

}
