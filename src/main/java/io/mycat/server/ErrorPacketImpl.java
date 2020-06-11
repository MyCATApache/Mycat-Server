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
 * https://mariadb.com/kb/en/library/err_packet/
 *
 * @author wuzhihui cjw
 */
public class ErrorPacketImpl implements ErrorPacket {
    private int errno;
    private int stage;
    private int maxStage;
    private int progress;
    private byte[] progress_info;
    private byte mark = ' ';
    private byte[] sqlState = DEFAULT_SQLSTATE;

    public int getErrorCode() {
        return errno;
    }

    public void setErrorCode(int errno) {
        this.errno = errno;
    }

    private String message;

  public void writePayload(MySQLPayloadWriteView buffer, int serverCapabilities) {
        buffer.writeByte((byte) 0xff);
        buffer.writeFixInt(2, errno);
        if (errno == 0xFFFF) { /* progress reporting */
            buffer.writeFixInt(1, stage);
            buffer.writeFixInt(1, maxStage);
            buffer.writeFixInt(3, progress);
            buffer.writeLenencString(progress_info);
        } else if (MySQLServerCapabilityFlags.isClientProtocol41(serverCapabilities)) {
            buffer.writeByte(mark);
            buffer.writeFixString(sqlState);
            buffer.writeEOFString(message);
        }else {
            buffer.writeEOFString(message);
        }
    }

    public void readPayload(MySQLPacket byteBuffer) {
        byte b = byteBuffer.readByte();
        assert (byte) 0xff == b;
        errno = (int) byteBuffer.readFixInt(2);
        if (errno == 0xFFFF) { /* progress reporting */
            stage = (int) byteBuffer.readFixInt(1);
            maxStage = (int) byteBuffer.readFixInt(1);
            progress = (int) byteBuffer.readFixInt(3);
            progress_info = byteBuffer.readLenencBytes();
        } else if (byteBuffer.getByte(byteBuffer.packetReadStartIndex()) == SQLSTATE_MARKER) {
            byteBuffer.skipInReading(1);
            mark = SQLSTATE_MARKER;
            sqlState = byteBuffer.readFixStringBytes(5);
        }
        message = byteBuffer.readEOFString();
    }

    public int getErrorStage() {
        return stage;
    }

    public void setErrorStage(int stage) {
        this.stage = stage;
    }

    public int getErrorMaxStage() {
        return maxStage;
    }

    public void setErrorMaxStage(int maxStage) {
        this.maxStage = maxStage;
    }

    public int getErrorProgress() {
        return progress;
    }

    public void setErrorProgress(int progress) {
        this.progress = progress;
    }

    public byte[] getErrorProgressInfo() {
        return progress_info;
    }

    public void setErrorProgressInfo(byte[] progress_info) {
        this.progress_info = progress_info;
    }

    public byte getErrorMark() {
        return mark;
    }

    public void setErrorMark(byte mark) {
        this.mark = mark;
    }

    public byte[] getErrorSqlState() {
        return sqlState;
    }

    public void setErrorSqlState(byte[] sqlState) {
        this.sqlState = sqlState;
    }

    public byte[] getErrorMessage() {
        return message.getBytes();
    }

    public void setErrorMessage(byte[] message) {
        this.message = new String(message);
    }
}
