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
package io.mycat.server.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.escape.Escapers.Builder;

import io.mycat.backend.mysql.BindValue;
import io.mycat.backend.mysql.ByteUtil;
import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.backend.mysql.nio.handler.PrepareRequestHandler;
import io.mycat.backend.mysql.nio.handler.PrepareRequestHandler.PrepareRequestCallback;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.net.handler.FrontendPrepareHandler;
import io.mycat.net.mysql.ExecutePacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.LongDataPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResetPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.response.PreparedStmtResponse;
import io.mycat.util.HexFormatUtil;


/**
 * @author mycat, CrazyPig, zhuam
 */
public class ServerPrepareHandler implements FrontendPrepareHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrepareHandler.class);
    
    private static Escaper varcharEscaper = null;

    static {
        Builder escapeBuilder = Escapers.builder();
        escapeBuilder.addEscape('\0', "\\0");
        escapeBuilder.addEscape('\'', "\\'");
        escapeBuilder.addEscape('\b', "\\b");
        escapeBuilder.addEscape('\n', "\\n");
        escapeBuilder.addEscape('\r', "\\r");
        escapeBuilder.addEscape('\"', "\\\"");
        escapeBuilder.addEscape('$', "\\$");
        escapeBuilder.addEscape('\\', "\\\\");
        varcharEscaper = escapeBuilder.build();
    }

    private ServerConnection source;

    // java int是32位，long是64位；mysql协议里面定义的statementId是32位，因此用Integer
    private static final AtomicInteger PSTMT_ID_GENERATOR = new AtomicInteger(0);
    //    private static final Map<String, PreparedStatement> pstmtForSql = new ConcurrentHashMap<>();
    private static final Map<Long, PreparedStatement> pstmtForId = new ConcurrentHashMap<>();
    private int maxPreparedStmtCount;

    public ServerPrepareHandler(ServerConnection source, int maxPreparedStmtCount) {
        this.source = source;
        this.maxPreparedStmtCount = maxPreparedStmtCount;
    }

    /**
     * 清理指定的预处理语句资源
     * @param pstmtId 预处理语句ID
     */
    public static void closePreparedStatement(long pstmtId) {
        LOGGER.info("Closing leaked preparestatement: {}", pstmtId);
        PreparedStatement pstmt = pstmtForId.remove(pstmtId);
        if (pstmt != null) {
            LOGGER.info("Successfully cleaned leaked preparestatement: {}", pstmtId);
        } else {
            LOGGER.debug("Already cleaned preparestatement: {}", pstmtId);
        }
    }

    @Override
    public void prepare(String sql) {

        LOGGER.debug("use server prepare, sql: " + sql);
        PreparedStatement pstmt = null;
        if (pstmt == null) {
            // 解析获取字段个数和参数个数
            int columnCount = 0;
            int paramCount = getParamCount(sql);
            if (paramCount > maxPreparedStmtCount) {
                source.writeErrMessage(ErrorCode.ER_PS_MANY_PARAM,
                        "Prepared statement contains too many placeholders");
                return;
            }
            pstmt = new PreparedStatement(PSTMT_ID_GENERATOR.incrementAndGet(), sql,
                    paramCount);
            pstmtForId.put(pstmt.getId(), pstmt);
            LOGGER.info("preparestatement prepare id:{} for connection:{}", pstmt.getId(), source.getId());
            
            // 添加到连接跟踪
            source.addPreparedStatementId(pstmt.getId());
        }
        PreparedStmtResponse.response(pstmt, source);
    }


    @Override
    public void sendLongData(byte[] data) {
        LongDataPacket packet = new LongDataPacket();
        packet.read(data);
        long pstmtId = packet.getPstmtId();
        LOGGER.info("preparestatement long data id:{}", pstmtId);
        PreparedStatement pstmt = pstmtForId.get(pstmtId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("send long data to prepare sql : " + pstmtForId.get(pstmtId));
            }
            long paramId = packet.getParamId();
            try {
                pstmt.appendLongData(paramId, packet.getLongData());
            } catch (IOException e) {
                source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPTION, e.getMessage());
            }
        }
    }

    @Override
    public void reset(byte[] data) {
        ResetPacket packet = new ResetPacket();
        packet.read(data);
        long pstmtId = packet.getPstmtId();
        LOGGER.info("preparestatement reset id:{}", pstmtId);
        PreparedStatement pstmt = pstmtForId.get(pstmtId);
        if (pstmt != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("reset prepare sql : " + pstmtForId.get(pstmtId));
            }
            pstmt.resetLongData();
            source.write(OkPacket.OK);
        } else {
            source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPTION,
                    "can not reset prepare statement : " + pstmtForId.get(pstmtId));
        }
    }

    @Override
    public void execute(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        LOGGER.info("preparestatement execute id:{}", pstmtId);
        if ((pstmt = pstmtForId.get(pstmtId)) == null) {
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND,
                    "Unknown pstmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.values;
            // 还原sql中的动态参数为实际参数值
            String sql = prepareStmtBindValue(pstmt, bindValues);
            // 执行sql
            source.getSession2().setPrepared(true);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("execute prepare sql: " + sql);
            }

            pstmt.resetLongData();
            source.query(sql);
        }
    }


    @Override
    public void close(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5); // 获取prepare stmt id
        LOGGER.info("preparestatement close id:{}", pstmtId);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("close prepare stmt, stmtId = " + pstmtId);
        }
        PreparedStatement pstmt = pstmtForId.remove(pstmtId);
        
        // 从连接跟踪中移除
        if (pstmt != null) {
            source.removePreparedStatementId(pstmtId);
        }
    }

    @Override
    public void clear() {
        this.pstmtForId.clear();
//    this.pstmtForSql.clear();
    }


    // 获取预处理sql中预处理参数个数
    private int getParamCount(String sql) {
        char[] cArr = sql.toCharArray();
        int count = 0;
        for (int i = 0; i < cArr.length; i++) {
            if (cArr[i] == '?') {
                count++;
            }
        }
        return count;
    }

    /**
     * 组装sql语句,替换动态参数为实际参数值
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
        String sql = pstmt.getStatement();
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        boolean primitiveArg = !(sqlStatement instanceof SQLSelectStatement);
        int[] paramTypes = pstmt.getParametersType();
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLVariantRefExpr x) {
                BindValue bindValue = bindValues[x.getIndex()];
                Object o = null;
                if (bindValue.isNull) {
                    SQLReplaceable parent = (SQLReplaceable) x.getParent();
                    parent.replace(x, new SQLNullExpr());
                    return false;
                } else {
                    if (primitiveArg && bindValue.value instanceof byte[]) {
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, new SQLHexExpr(HexFormatUtil.bytesToHexString((byte[]) bindValue.value)));
                        return false;
                    }
                    switch (paramTypes[x.getIndex()] & 0xff) {
                        case Fields.FIELD_TYPE_TINY:
                            o = bindValue.byteBinding;
                            break;
                        case Fields.FIELD_TYPE_SHORT:
                            o = bindValue.shortBinding;
                            break;
                        case Fields.FIELD_TYPE_LONG:
                            o = bindValue.intBinding;
                            break;
                        case Fields.FIELD_TYPE_LONGLONG:
                            o = (bindValue.longBinding);
                            break;
                        case Fields.FIELD_TYPE_FLOAT:
                            o = bindValue.floatBinding;
                            break;
                        case Fields.FIELD_TYPE_DOUBLE:
                            o = bindValue.doubleBinding;
                            break;
                        case Fields.FIELD_TYPE_TIME:
                        case Fields.FIELD_TYPE_DATE:
                        case Fields.FIELD_TYPE_DATETIME:
                        case Fields.FIELD_TYPE_TIMESTAMP:
                            o = bindValue.value;
                            break;
                        default:
                            if (bindValue.value instanceof byte[]) {
                                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                                byte[] bytes = (byte[]) bindValue.value;
                                parent.replace(x, new SQLCharExpr(new String(bytes)));
                                return false;
                            }
                            if (bindValue.value instanceof String) {
                                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                                String value = (String) bindValue.value;
                                parent.replace(x, new SQLCharExpr(value));
                                return false;
                            }
                            throw new UnsupportedOperationException("unsupport " + bindValue.value + " class:" + Optional.ofNullable(bindValue.value).map(i -> i.getClass()).orElse(null));
                    }

                    SQLReplaceable parent = (SQLReplaceable) x.getParent();
                    parent.replace(x, SQLExprUtils.fromJavaObject(o));
                    return false;
                }
            }

        });
        return sqlStatement.toString();
    }
}