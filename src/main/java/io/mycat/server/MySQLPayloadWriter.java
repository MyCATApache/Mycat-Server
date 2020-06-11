package io.mycat.server;


import java.io.Closeable;

/**
 * @author jamie12221
 *  date 2019-05-07 21:47
 * 默认mysql packet 写视图实现
 * //todo 需要高性能实现
 **/
public class MySQLPayloadWriter extends ByteArrayOutput implements
    MySQLPayloadWriteView<MySQLPayloadWriter>, Closeable {

  //private final PacketSplitterImpl packetSplitter = new PacketSplitterImpl();

  public MySQLPayloadWriter() {
  }

  public MySQLPayloadWriter(int size) {
    super(size);
  }

  @Override
  public MySQLPayloadWriter writeLong(long x) {
    writeByte(long0(x));
    writeByte(long1(x));
    writeByte(long2(x));
    writeByte(long3(x));
    writeByte(long4(x));
    writeByte(long5(x));
    writeByte(long6(x));
    writeByte(long7(x));
    return this;
  }

  @Override
  public MySQLPayloadWriter writeFixInt(int length, long val) {
    for (int i = 0; i < length; i++) {
      byte b = (byte) ((val >>> (i * 8)) & 0xFF);
      writeByte(b);
    }
    return this;
  }

  private static byte short1(short x) {
    return (byte) (x >> 8);
  }

  @Override
  public MySQLPayloadWriter writeFixString(String val) {
    writeFixString(val.getBytes());
    return this;
  }

  @Override
  public MySQLPayloadWriter writeFixString(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
    return this;
  }

  @Override
  public MySQLPayloadWriter writeLenencBytesWithNullable(byte[] bytes) {
    byte nullVal = 0;
    if (bytes == null) {
      writeByte(nullVal);
    } else {
      writeLenencBytes(bytes);
    }
    return this;
  }

  @Override
  public MySQLPayloadWriter writeLenencString(byte[] bytes) {
    return writeLenencBytes(bytes);
  }

  @Override
  public MySQLPayloadWriter writeLenencString(String val) {
    return writeLenencBytes(val.getBytes());
  }

  public MySQLPayloadWriter writeBytes(byte[] bytes) {
    write(bytes, 0, bytes.length);
    return this;
  }

  @Override
  public MySQLPayloadWriter writeBytes(byte[] bytes, int offset, int length) {
    write(bytes, offset, length);
    return this;
  }


  @Override
  public MySQLPayloadWriter writeNULString(String val) {
    return writeNULString(val.getBytes());
  }

  @Override
  public MySQLPayloadWriter writeNULString(byte[] vals) {
    writeFixString(vals);
    writeByte(0);
    return this;
  }

  @Override
  public MySQLPayloadWriter writeEOFString(String val) {
    return writeFixString(val);
  }

  @Override
  public MySQLPayloadWriter writeEOFStringBytes(byte[] bytes) {
    return writeBytes(bytes, 0, bytes.length);
  }

  @Override
  public MySQLPayloadWriter writeLenencBytes(byte[] bytes) {
    writeLenencInt(bytes.length);
    writeBytes(bytes);
    return this;
  }

  @Override
  public MySQLPayloadWriter writeLenencBytes(byte[] bytes, byte[] nullValue) {
    if (bytes == null) {
      return writeLenencBytes(nullValue);
    } else {
      return writeLenencBytes(bytes);
    }
  }

  @Override
  public MySQLPayloadWriter writeByte(byte val) {
    write(val);
    return this;
  }

  @Override
  public MySQLPayloadWriter writeReserved(int length) {
    for (int i = 0; i < length; i++) {
      writeByte(0);
    }
    return this;
  }

  @Override
  public MySQLPayloadWriter writeDouble(double d) {
    writeLong(Double.doubleToRawLongBits(d));
    return this;
  }

  private static byte long7(long x) {
    return (byte) (x >> 56);
  }

  private static byte long6(long x) {
    return (byte) (x >> 48);
  }

  private static byte long5(long x) {
    return (byte) (x >> 40);
  }

  private static byte long4(long x) {
    return (byte) (x >> 32);
  }

  private static byte long3(long x) {
    return (byte) (x >> 24);
  }

  private static byte long2(long x) {
    return (byte) (x >> 16);
  }

  private static byte long1(long x) {
    return (byte) (x >> 8);
  }

  private static byte long0(long x) {
    return (byte) (x);
  }

  private static byte short0(short x) {
    return (byte) (x);
  }

  private static byte int3(int x) {
    return (byte) (x >> 24);
  }

  private static byte int2(int x) {
    return (byte) (x >> 16);
  }

  private static byte int1(int x) {
    return (byte) (x >> 8);
  }

  private static byte int0(int x) {
    return (byte) (x);
  }

  @Override
  public MySQLPayloadWriter writeLenencInt(long val) {
    if (val < 251) {
      writeByte((byte) val);
    } else if (val >= 251 && val < (1 << 16)) {
      writeByte((byte) 0xfc);
      writeFixInt(2, val);
    } else if (val >= (1 << 16) && val < (1 << 24)) {
      writeByte((byte) 0xfd);
      writeFixInt(3, val);
    } else {
      writeByte((byte) 0xfe);
      writeFixInt(8, val);
    }
    return this;
  }

  public void writeShort(short o) {
    write(short0(o));
    write(short1(o));
  }

  public void writeFloat(Float o) {
    writeInt(Float.floatToIntBits(o));
  }

  public void writeInt(int o) {
    write(int0(o));
    write(int1(o));
    write(int2(o));
    write(int3(o));

  }
}
