package io.mycat.net.mysql;

import java.nio.ByteBuffer;

import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.config.Capabilities;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.net.FrontendConnection;


/**
 * 
 * 1              [fe]
 * string[NUL]    plugin name
 * string[EOF]    auth plugin data
 *  
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate Jun 18, 2021 <br>
 * @since V8.1<br>
 * @see io.mycat.net.mysql <br>
 */
public class AuthSwitchPacket extends MySQLPacket{
    
    private static final byte STATUS = (byte) 0XFE;
    private byte[] authMethodName ;
    private byte[] authMethodData;

    public AuthSwitchPacket(byte[] authMethodName, byte[] authMethodData) {
        super();
        this.authMethodName = authMethodName;
        this.authMethodData = authMethodData;
    }
    
    public AuthSwitchPacket() {
    }
    
    
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        authMethodData = mm.readBytes(packetLength);
    }
    
    
    @Override
    public ByteBuffer write(ByteBuffer buffer, FrontendConnection c,
            boolean writeSocketIfFull) {
        int size = calcPacketSize();
        buffer = c.checkWriteBuffer(buffer, c.getPacketHeaderSize() + size,
                writeSocketIfFull);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(STATUS);
        BufferUtil.writeWithNull(buffer, authMethodName);
        BufferUtil.writeWithNull(buffer, authMethodData);
        return buffer;
    }



    public void write(FrontendConnection c) {
        ByteBuffer buffer = c.allocate();
        buffer = this.write(buffer, c, true);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        int size = 3; //status
        size += authMethodName.length;
        size += authMethodData.length;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Auth Switch Packet";
    }

    public byte[] getAuthMethodName() {
        return authMethodName;
    }

    public void setAuthMethodName(byte[] authMethodName) {
        this.authMethodName = authMethodName;
    }

    public byte[] getAuthMethodData() {
        return authMethodData;
    }

    public void setAuthMethodData(byte[] authMethodData) {
        this.authMethodData = authMethodData;
    }
}
