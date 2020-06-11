/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.server;

/**
 * cjw
 * 294712221@qq.com
 */
public class MySQLServerCapabilityFlags {
    public int value = 0;


    public static int getDefaultServerCapabilities() {
        int flag = 0;
        flag |= MySQLServerCapabilityFlags.CLIENT_LONG_PASSWORD;
        flag |= MySQLServerCapabilityFlags.CLIENT_FOUND_ROWS;
        flag |= MySQLServerCapabilityFlags.CLIENT_LONG_FLAG;
        flag |= MySQLServerCapabilityFlags.CLIENT_CONNECT_WITH_DB;
//        flag |= MySQLServerCapabilityFlags.CLIENT_NO_SCHEMA;
        // boolean usingCompress = MycatServer.getInstance().getConfig()
        // .getSystem().getUseCompression() == 1;
        // if (usingCompress) {
        // flag |= MySQLServerCapabilityFlags.CLIENT_COMPRESS;
        // }
//        flag |= MySQLServerCapabilityFlags.CLIENT_ODBC;
        flag |= MySQLServerCapabilityFlags.CLIENT_LOCAL_FILES;
        flag |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SPACE;
        flag |= MySQLServerCapabilityFlags.CLIENT_PROTOCOL_41;
        flag |= MySQLServerCapabilityFlags.CLIENT_INTERACTIVE;
        // flag |= MySQLServerCapabilityFlags.CLIENT_SSL;
        flag |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SIGPIPE;
        flag |= MySQLServerCapabilityFlags.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= MySQLServerCapabilityFlags.CLIENT_SECURE_CONNECTION;
        flag |= MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH;
//        flag |= MySQLServerCapabilityFlags.CLIENT_CONNECT_ATTRS;
//        flag |= MySQLServerCapabilityFlags.CLIENT_DEPRECATE_EOF;
        flag &=~ MySQLServerCapabilityFlags.CLIENT_SESSION_TRACK;
        return flag;
    }



    /**
     * server value
     *
     *
     * server:        11110111 11111111
     * client_cmd: 11 10100110 10000101
     * client_jdbc:10 10100010 10001111
     *
     * see  'http://dev.mysql.com/doc/refman/5.1/en/mysql-real-connect.html'
     *
     */
    // new more secure passwords
    public static final int CLIENT_LONG_PASSWORD = 1;

    // Found instead of affected rows
    // 返回找到（匹配）的行数，而不是改变了的行数。
    public static final int CLIENT_FOUND_ROWS = 2;

    // Get all column flags
    public static final int CLIENT_LONG_FLAG = 4;

    // One can specify db on connect
    public static final int CLIENT_CONNECT_WITH_DB = 8;

    // Don't allow database.table.column
    // 不允许“数据库名.表名.列名”这样的语法。这是对于ODBC的设置。
    // 当使用这样的语法时解析器会产生一个错误，这对于一些ODBC的程序限制bug来说是有用的。
    public static final int CLIENT_NO_SCHEMA = 16;

    // Can use compression protocol
    // 使用压缩协议
    public static final int CLIENT_COMPRESS = 32;

    // Odbc client
    public static final int CLIENT_ODBC = 64;

    // Can use LOAD DATA LOCAL
    public static final int CLIENT_LOCAL_FILES = 128;

    // Ignore spaces before '('
    // 允许在函数名后使用空格。所有函数名可以预留字。
    public static final int CLIENT_IGNORE_SPACE = 256;

    // New 4.1 protocol This is an interactive client
    public static final int CLIENT_PROTOCOL_41 = 512;

    // This is an interactive client
    // 允许使用关闭连接之前的不活动交互超时的描述，而不是等待超时秒数。
    // 客户端的会话等待超时变量变为交互超时变量。
    public static final int CLIENT_INTERACTIVE = 1024;

    // Switch to SSL after handshake
    // 使用SSL。这个设置不应该被应用程序设置，他应该是在客户端库内部是设置的。
    // 可以在调用mysql_real_connect()之前调用mysql_ssl_set()来代替设置。
    public static final int CLIENT_SSL = 2048;

    // IGNORE sigpipes
    // 阻止客户端库安装一个SIGPIPE信号处理器。
    // 这个可以用于当应用程序已经安装该处理器的时候避免与其发生冲突。
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;

    // Client knows about transactions
    public static final int CLIENT_TRANSACTIONS = 8192;

    // Old flag for 4.1 protocol
    public static final int CLIENT_RESERVED = 16384;

    // New 4.1 authentication
    public static final int CLIENT_SECURE_CONNECTION = 32768;

    // Enable/disable multi-stmt support
    // 通知服务器客户端可以发送多条语句（由分号分隔）。如果该标志为没有被设置，多条语句执行。
    public static final int CLIENT_MULTI_STATEMENTS = 1<<16;

    // Enable/disable multi-results
    // 通知服务器客户端可以处理由多语句或者存储过程执行生成的多结果集。
    // 当打开CLIENT_MULTI_STATEMENTS时，这个标志自动的被打开。
    public static final int CLIENT_MULTI_RESULTS = 1<<1<<16;

    /**
     ServerCan send multiple resultsets for COM_STMT_EXECUTE.
     Client
     Can handle multiple resultsets for COM_STMT_EXECUTE.
     Value
     0x00040000
     Requires
     CLIENT_PROTOCOL_41
     */
    public static final int  CLIENT_PS_MULTI_RESULTS = 1<<2<<16;
    /**
     Server
     Sends extra data in Initial Handshake Packet and supports the pluggable authentication protocol.

     Client
     Supports authentication plugins.

     Requires
     CLIENT_PROTOCOL_41
     */
    public static final int CLIENT_PLUGIN_AUTH = 1<<3<<16;

    /**
     Value
     0x00100000

     Server
     Permits connection attributes in Protocol::HandshakeResponse41.

     Client
     Sends connection attributes in Protocol::HandshakeResponse41.
     */

    public static final int CLIENT_CONNECT_ATTRS = 1<<4<<16;

    /**
     Value
     0x00200000

     Server
     Understands length-encoded integer for auth response data in Protocol::HandshakeResponse41.

     Client
     Length of auth response data in Protocol::HandshakeResponse41 is a length-encoded integer.
     The flag was introduced in 5.6.6, but had the wrong value.
     */
    public static final int CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 1<<5<<16;

    /**
     *Value
     * 0x00400000
     *
     * Server
     * Announces support for expired password extension.
     *
     * Client
     * Can handle expired passwords.
     */
    public static final int CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1<<6<<16;

    /**
     * Value
     * 0x00800000
     *
     * Server
     * Can set SERVER_SESSION_STATE_CHANGED in the Status Flags and send session-state change data after a OK packet.
     *
     * Client
     * Expects the server to send sesson-state changes after a OK packet.
     */
    public static final int CLIENT_SESSION_TRACK = 1<<7<<16;

    /**
     Value
     0x01000000

     Server
     Can send OK after a Text Resultset.

     Client
     Expects an OK (instead of EOF) after the resultset rows of a Text Resultset.

     Background
     To support CLIENT_SESSION_TRACK, additional information must be sent after all successful commands. Although the OK packet is extensible, the EOF packet is not due to the overlap of its bytes with the content of the Text Resultset Row.

     Therefore, the EOF packet in the Text Resultset is replaced with an OK packet. EOF packets are deprecated as of MySQL 5.7.5.
     */
    public static final int CLIENT_DEPRECATE_EOF = 1<<8<<16;



    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MySQLServerCapabilityFlags{");
        sb.append("value=").append(Integer.toBinaryString(value << 7));
        sb.append('}');
        return sb.toString();
    }

    public MySQLServerCapabilityFlags(int capabilities) {
        this.value = capabilities;
    }

    public MySQLServerCapabilityFlags() {
    }

    public int getLower2Bytes() {
        return value & 0x0000ffff;
    }

    public int getUpper2Bytes() {
        return value >>> 16;
    }

    public boolean isLongPassword() {
        return isLongPassword(value);
    }

    public static boolean isLongPassword(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_LONG_PASSWORD) != 0;
    }

    public void setLongPassword() {
        value |= MySQLServerCapabilityFlags.CLIENT_LONG_PASSWORD;
    }

    public boolean isFoundRows() {
        return isFoundRows(value);
    }

    public static boolean isFoundRows(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_FOUND_ROWS) != 0;
    }

    public void setFoundRows() {
        value |= MySQLServerCapabilityFlags.CLIENT_FOUND_ROWS;
    }

    public boolean isLongColumnWithFLags() {
        return isLongColumnWithFLags(value);
    }

    public boolean isLongColumnWithFLags(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_LONG_FLAG) != 0;
    }

    public void setLongColumnWithFLags() {
        value |= MySQLServerCapabilityFlags.CLIENT_LONG_FLAG;
    }

    public boolean isConnectionWithDatabase() {
        return isConnectionWithDatabase(value);
    }

    public static boolean isConnectionWithDatabase(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_CONNECT_WITH_DB) != 0;
    }

    public void setConnectionWithDatabase() {
        value |= MySQLServerCapabilityFlags.CLIENT_CONNECT_WITH_DB;
    }

    public boolean isDoNotAllowDatabaseDotTableDotColumn() {
        return isDoNotAllowDatabaseDotTableDotColumn(value);
    }

    public static boolean isDoNotAllowDatabaseDotTableDotColumn(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_NO_SCHEMA) != 0;
    }

    public void setDoNotAllowDatabaseDotTableDotColumn() {
        value |= MySQLServerCapabilityFlags.CLIENT_NO_SCHEMA;
    }

    public boolean isCanUseCompressionProtocol() {
        return isCanUseCompressionProtocol(value);
    }

    public static boolean isCanUseCompressionProtocol(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_COMPRESS) != 0;
    }

    public void setCanUseCompressionProtocol() {
        value |= MySQLServerCapabilityFlags.CLIENT_COMPRESS;
    }

    public boolean isODBCClient() {
        return isODBCClient(value);
    }

    public static boolean isODBCClient(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_ODBC) != 0;
    }

    public void setODBCClient() {
        value |= MySQLServerCapabilityFlags.CLIENT_ODBC;
    }

    public boolean isCanUseLoadDataLocal() {
        return isCanUseLoadDataLocal(value);
    }

    public static boolean isCanUseLoadDataLocal(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_LOCAL_FILES) != 0;
    }

    public void setCanUseLoadDataLocal() {
        value |= MySQLServerCapabilityFlags.CLIENT_LOCAL_FILES;
    }

    public boolean isIgnoreSpacesBeforeLeftBracket() {
        return isIgnoreSpacesBeforeLeftBracket(value);
    }

    public static boolean isIgnoreSpacesBeforeLeftBracket(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_IGNORE_SPACE) != 0;
    }

    public void setIgnoreSpacesBeforeLeftBracket() {
        value |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SPACE;
    }

    public boolean isClientProtocol41() {
        return isClientProtocol41(value);
    }

    public static boolean isClientProtocol41(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_PROTOCOL_41) != 0;
    }

    public void setClientProtocol41() {
        value |= MySQLServerCapabilityFlags.CLIENT_PROTOCOL_41;
    }

    public boolean isSwitchToSSLAfterHandshake() {
        return isSwitchToSSLAfterHandshake(value);
    }

    public static boolean isSwitchToSSLAfterHandshake(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_SSL) != 0;
    }

    public void setSwitchToSSLAfterHandshake() {
        value |= MySQLServerCapabilityFlags.CLIENT_SSL;
    }

    public boolean isIgnoreSigpipes() {
        return isIgnoreSigpipes(value);
    }

    public static boolean isIgnoreSigpipes(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_IGNORE_SIGPIPE) != 0;
    }

    public void setIgnoreSigpipes() {
        value |= MySQLServerCapabilityFlags.CLIENT_IGNORE_SIGPIPE;
    }

    public boolean isKnowsAboutTransactions() {
        return isKnowsAboutTransactions(value);
    }

    public static boolean isKnowsAboutTransactions(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_TRANSACTIONS) != 0;
    }

    public void setKnowsAboutTransactions() {
        value |= MySQLServerCapabilityFlags.CLIENT_TRANSACTIONS;
    }


    public void setInteractive() {
        value |= MySQLServerCapabilityFlags.CLIENT_INTERACTIVE;
    }

    public boolean isInteractive() {
        return isInteractive(value);
    }

    public static boolean isInteractive(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_INTERACTIVE) != 0;
    }

    public boolean isSpeak41Protocol() {
        return isSpeak41Protocol(value);
    }

    public static boolean isSpeak41Protocol(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_RESERVED) != 0;
    }

    public void setSpeak41Protocol() {
        value |= MySQLServerCapabilityFlags.CLIENT_RESERVED;
    }

    public boolean isCanDo41Anthentication() {
        return isCanDo41Anthentication(value);
    }

    public static boolean isCanDo41Anthentication(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_SECURE_CONNECTION) != 0;
    }

    public void setCanDo41Anthentication() {
        value |= MySQLServerCapabilityFlags.CLIENT_SECURE_CONNECTION;
    }


    public boolean isMultipleStatements() {
        return isMultipleStatements(value);
    }

    public static boolean isMultipleStatements(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_MULTI_STATEMENTS) != 0;
    }

    public void setMultipleStatements() {
        value |= MySQLServerCapabilityFlags.CLIENT_MULTI_STATEMENTS;
    }

    public boolean isMultipleResults() {
        return isMultipleResults(value);
    }

    public boolean isMultipleResults(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_MULTI_RESULTS) != 0;
    }

    public void setMultipleResults() {
        value |= MySQLServerCapabilityFlags.CLIENT_MULTI_RESULTS;
    }

    public boolean isPSMultipleResults() {
        return isPSMultipleResults(value);
    }

    public static boolean isPSMultipleResults(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_PS_MULTI_RESULTS) != 0;
    }

    public void setPSMultipleResults() {
        value |= MySQLServerCapabilityFlags.CLIENT_PS_MULTI_RESULTS;
    }

    public boolean isPluginAuth() {
        return isPluginAuth(value);
    }

    public static boolean isPluginAuth(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH) != 0;
    }

    public void setPluginAuth() {
        value |= MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH;
    }

    public boolean isConnectAttrs() {
        return isConnectAttrs(value);
    }

    public static boolean isConnectAttrs(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_CONNECT_ATTRS) != 0;
    }

    public void setConnectAttrs() {
        value |= MySQLServerCapabilityFlags.CLIENT_CONNECT_ATTRS;
    }

    public boolean isPluginAuthLenencClientData() {
        return isPluginAuthLenencClientData(value);
    }

    public static boolean isPluginAuthLenencClientData(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0;
    }

    public void setPluginAuthLenencClientData() {
        value |= MySQLServerCapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }

    public boolean isClientCanHandleExpiredPasswords() {
        return isClientCanHandleExpiredPasswords(value);
    }

    public boolean isClientCanHandleExpiredPasswords(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS) != 0;
    }

    public void setClientCanHandleExpiredPasswords() {
        value |= MySQLServerCapabilityFlags.CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS;
    }

    public boolean isSessionVariableTracking() {
        return isSessionVariableTracking(value);
    }

    public static boolean isSessionVariableTracking(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_SESSION_TRACK) != 0;
    }

    public void setSessionVariableTracking() {
        value |= MySQLServerCapabilityFlags.CLIENT_SESSION_TRACK;
    }

    public boolean isDeprecateEOF() {
        return isDeprecateEOF(value);
    }

    public static boolean isDeprecateEOF(int value) {
        return (value & MySQLServerCapabilityFlags.CLIENT_DEPRECATE_EOF) != 0;
    }

    public void setDeprecateEOF() {
        value |= MySQLServerCapabilityFlags.CLIENT_DEPRECATE_EOF;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MySQLServerCapabilityFlags)) return false;

        MySQLServerCapabilityFlags that = (MySQLServerCapabilityFlags) o;

        return hashCode() == that.hashCode();
    }

    @Override
    public int hashCode() {
        return value << 7;
    }


}
