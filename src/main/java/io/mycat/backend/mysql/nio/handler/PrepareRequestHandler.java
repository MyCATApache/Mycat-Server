package io.mycat.backend.mysql.nio.handler;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.net.mysql.CommandPacket;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.MySQLPacket;
import io.mycat.net.mysql.PreparedOkPacket;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.ServerConnection;
import io.mycat.util.StringUtil;

/**
 * 用于取prepare语句的原数据，直接把语句透传给Mysql
 * https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
 * 
 * 约束： 不支持库内分表等需要改写表名的语句
 * 
 * @author funnyAnt
 *
 */
public class PrepareRequestHandler implements ResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareRequestHandler.class);

    private String sql;
    private ServerConnection sc;
    private PrepareRequestCallback callbackHandler;
    private volatile long statementId = -1L;
    private List<FieldPacket> params;
    private List<FieldPacket> fields;
    private volatile boolean statementClosed;
    private int paramSize;
    private int fieldSize;
    private volatile boolean lastPacket;
    private static String SYNC_CHANNEL_STATUS_SQL = "SELECT 1";// 用于同步通道的状态信息

    public PrepareRequestHandler(ServerConnection sc, String sql, PrepareRequestCallback callbackHandler) {
        this.sc = sc;
        this.sql = sql;
        this.callbackHandler = callbackHandler;
        this.params = new ArrayList<>(8);
        this.fields = new ArrayList<>(8);
    }

    public void execute() {
        try {
            List<String> tables = parseTables(sql);
            SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(sc.getSchema());
            // 根据表的规则确定路由节点，路由判断规则时剔除全局表后，选择剩余第一个表的第一个节点
            String routeDataNode = null;
            for (String table : tables) {
                TableConfig tc = schemaConfig.getTables().get(table.toUpperCase());
                if (tc != null) {
                    routeDataNode = tc.getDataNodes().get(0);
                } else {
                    routeDataNode = schemaConfig.getDataNode();
                }

                if (tc != null && !tc.isGlobalTable()) {
                    break;
                }
            }
            if (routeDataNode == null) {
                doFinished(true, "can't find route node");
                return;
            }

            PhysicalDBNode dn = MycatServer.getInstance().getConfig().getDataNodes().get(routeDataNode);
            PhysicalDatasource ds = dn.getDbPool().getSource();
            if (ds.getConfig().getDbType().equalsIgnoreCase("MYSQL")) {
                ds.getConnection(dn.getDatabase(), true, this, null);
            } else {
                doFinished(true, "Does not support getting metadata from non-mysql database");
            }
        } catch (Exception e) {
            LOGGER.info("can't get connection for sql ,error:", e);
            doFinished(true, e.getMessage());
        }
    }

    private void doFinished(boolean failed, String errorMsg) {
        if (!lastPacket) {
            lastPacket = true;
            callbackHandler.callback(!failed, errorMsg, params.toArray(new FieldPacket[0]),
                    fields.toArray(new FieldPacket[0]));
        }
    }

    private void closePrepareStmt(BackendConnection conn) {
        //https://dev.mysql.com/doc/internals/en/com-stmt-close.html
        if (!statementClosed) {
            // PreparedClosePacket packet = new PreparedClosePacket(this.statementId);
            CommandPacket closePreparePacket = new CommandPacket();
            closePreparePacket.command = MySQLPacket.COM_STMT_CLOSE;
            closePreparePacket.arg = String.valueOf(this.statementId).getBytes();
            statementClosed = true;
            closePreparePacket.write((MySQLConnection) conn);
        }

    }
    /**
     * 从sql 语句中解析出tableName
     * 
     * @return
     */
    private List<String> parseTables(String sql) {
        List<String> tables = new ArrayList<>();
        try {
            SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);
            SQLStatement statement = sqlStatementParser.parseStatement();
            if (statement instanceof MySqlUpdateStatement){
                String simpleName = ((MySqlUpdateStatement) statement).getTableName().getSimpleName();
                tables.add(StringUtil.removeBackquote(simpleName));
            }else if (statement instanceof MySqlDeleteStatement){
                String simpleName = ((MySqlDeleteStatement) statement).getTableName().getSimpleName();
                tables.add(StringUtil.removeBackquote(simpleName));
            }else if (statement instanceof MySqlInsertStatement){
                String simpleName = ((MySqlInsertStatement) statement).getTableName().getSimpleName();
                tables.add(StringUtil.removeBackquote(simpleName));
            } else {
                MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();
                statement.accept(visitor);
                tables.addAll(visitor.getAliasMap().values());
            }
        } catch (Exception  e) {
            LOGGER.error("can not get column count", e);
        }
        return tables;
    }

    private void sendPrepareRequestCommand(BackendConnection conn) {
        try {
            CommandPacket preparePacket = new CommandPacket();
            preparePacket.command = MySQLPacket.COM_STMT_PREPARE;
            preparePacket.arg = this.sql.getBytes(sc.getCharset());
            preparePacket.write((MySQLConnection) conn);
        } catch (Exception e) {
            doFinished(true, e.getMessage());
        }
    }

    public boolean isLastPacket() {
        return lastPacket;
    }

    @Override
    public void connectionError(Throwable e, BackendConnection conn) {
        // TODO Auto-generated method stub
        LOGGER.info("can't get connection for sql :" + sql);
        doFinished(true, e.getMessage());
    }

    @Override
    public void connectionAcquired(BackendConnection conn) {
        // TODO Auto-generated method stub
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("con query sql:" + sql + " to con:" + conn);
        }
        conn.setResponseHandler(this);

        try {
            if (((MySQLConnection) conn).isNeedSyncSchema()) {
                // 发送一个select 1语句触发同步schema；后面在rowEofResponse里面发送PrepareRequestCommand命令
                conn.query(SYNC_CHANNEL_STATUS_SQL);
            }else {
                this.sendPrepareRequestCommand(conn);
            }
            
        } catch (Exception e) {// (UnsupportedEncodingException e) {
            doFinished(true, e.getMessage());
        }
    }

    @Override
    public void errorResponse(byte[] err, BackendConnection conn) {
        ErrorPacket errPg = new ErrorPacket();
        errPg.read(err);

        String errMsg = "error response errno:" + errPg.errno + ", " + new String(errPg.message) + " from of sql :"
                + sql + " at con:" + conn;

        // @see https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html
        // ER_SPECIFIC_ACCESS_DENIED_ERROR
        if (errPg.errno == 1227) {
            LOGGER.warn(errMsg);

        } else {
            LOGGER.info(errMsg);
        }

        doFinished(true, errMsg);
        // closePrepareStmt(conn);
        conn.release();

    }

    @Override
    public void okResponse(byte[] data, BackendConnection conn) {
         // TODO Auto-generated method stub
        if(statementId == -1L) {
            boolean executeResponse = conn.syncAndExcute();
            if (executeResponse) {
                // 解析包
                PreparedOkPacket preparedOkPacket = new PreparedOkPacket();
                preparedOkPacket.read(data);
                this.paramSize = preparedOkPacket.parametersNumber;
                this.fieldSize = preparedOkPacket.columnsNumber;
                this.statementId = preparedOkPacket.statementId;
          }
        }else {
            switch (data[4]) {
            case ErrorPacket.FIELD_COUNT:
                errorResponse(data, conn);
                break;
            case EOFPacket.FIELD_COUNT:
                if (fields.size() == fieldSize) {
                    // field包后面的EOF包,整个协议结束。
                    doFinished(false, null);
                    closePrepareStmt(conn);
                    conn.release();
                }
                break;
            default:
                // 解析field
                FieldPacket packet = new FieldPacket();
                packet.read(data);
                // 数据库名称修改为mycat逻辑库
                packet.db = this.sc.getSchema().getBytes();
                packet.length = packet.calcPacketSize();

                if (params.size() < paramSize) {
                    params.add(packet);
                } else {
                    if (fields.size() < fieldSize) {
                        fields.add(packet);
                    }
                }

            }
        }
    }

    @Override
    public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
        // TODO Auto-generated method stub

    }

    @Override
    public void rowResponse(byte[] row, BackendConnection conn) {
        // TODO Auto-generated method stub
    }

    @Override
    public void rowEofResponse(byte[] eof, BackendConnection conn) {
        // TODO Auto-generated method stub
        // 说明同步通道的语句SELECT 1已经成功，发送真正的prepare包。这样做的原因是包类型不一样，目前接口无法做到一次性发送
        this.sendPrepareRequestCommand(conn);
    }

    @Override
    public void writeQueueAvailable() {
        // TODO Auto-generated method stub

    }

    @Override
    public void connectionClose(BackendConnection conn, String reason) {
        // TODO Auto-generated method stub
        doFinished(true, reason);
    }

    /**
     * 
     * 用于回调返回PrepareRequest值
     *
     */
    public interface PrepareRequestCallback {
        void callback(boolean success, String msg, FieldPacket[] params, FieldPacket[] fields);
    }
}
