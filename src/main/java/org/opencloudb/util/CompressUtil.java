package org.opencloudb.util;

import com.google.common.collect.Lists;
import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.MySQLMessage;
import org.opencloudb.net.AbstractConnection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class CompressUtil
{

     public int MINI_LENGTH_TO_COMPRESS=50;

    public static ByteBuffer compressMysqlPacket(ByteBuffer input,AbstractConnection con)
    {
        if(true)
        {
            return input;
        }
      return compressMysqlPacket(getByteArrayFromBuffer(input),con);
    }

    public static ByteBuffer compressMysqlPacket(byte[] data,AbstractConnection con)
    {

        ByteBuffer byteBuf=con.allocate();

        byteBuf=con.checkWriteBuffer(byteBuf,data.length+7,false);
        byte packID=data[3];
        if(data.length<=50)
        {
            BufferUtil.writeUB3(byteBuf, data.length);
            byteBuf.put(packID);
            BufferUtil.writeUB3(byteBuf, 0);
            byteBuf.put(data);
        }   else
        {
            byte[] compress = compress(data);
            BufferUtil.writeUB3(byteBuf, compress.length);
            byteBuf.put(packID);
            BufferUtil.writeUB3(byteBuf, data.length);
            byteBuf.put(compress);
        }
        return byteBuf;
    }

    public static List<byte[]> decompressMysqlPacket(byte[] data)
    {
        MySQLMessage mm = new MySQLMessage(data);
        int len=   mm.readUB3();
        byte  packetId = mm.read();
        int oldLen=   mm.readUB3();
        if(len==data.length-4)
        {
            return Lists.newArrayList(data);
        }
      else
        if(oldLen==0)
        {
            return  Lists.newArrayList(mm.readBytes());
        }
        else
       {
           byte[] de= decompress(data,7,data.length-7);
           return  splitPack(de);
       }
    }

    private static List<byte[]> splitPack(byte[] in)
    {
        List<byte[]> rtn=new ArrayList<>();
        MySQLMessage msg=new MySQLMessage(in);
        while (msg.hasRemaining())
        {
            int i=msg.readUB3();
            msg.move(-3);
           rtn.add(msg.readBytes(i + 4));
        }


        return rtn;
    }


    private static byte[] getByteArrayFromBuffer(ByteBuffer byteBuf)
  {
      byteBuf.flip();
      byte[] row = new byte[byteBuf.limit()];
      byteBuf.get(row);
      byteBuf.clear();
      return row;
  }
    public static byte[] compress(ByteBuffer byteBuf)
    {
        return compress(getByteArrayFromBuffer(byteBuf)) ;
    }


    public static byte[] compress(byte[] data)  {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        try
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e)
        {
           throw new RuntimeException(e);
        }finally
        {
            deflater.end();
        }

    }

    /**
     * 适用于mysql与客户端交互时zlib解压
     *
     * @param data
     * @param off
     * @param len
     * @param out
     * @throws Exception
     */
    public static void decompress(byte[] data, int off, int len, OutputStream out)
    {
        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data, off, len);
        byte[] buf = new byte[1024];

        try
        {
            while (!decompresser.finished())
            {
                int i = decompresser.inflate(buf);
                out.write(buf, 0, i);
                out.flush();
            }
        } catch (Exception ex)
        {
            throw new RuntimeException(ex);
        } finally
        {
            decompresser.end();
        }
    }

    /**
     * 适用于mysql与客户端交互时zlib解压
     *
     * @param data 数据
     * @param off  偏移量
     * @param len  长度
     * @return
     */
    public static byte[] decompress(byte[] data, int off, int len)
    {
        byte[] output = null;
        Inflater decompresser = new Inflater();
        decompresser.reset();
//		decompresser.setInput(data);
        decompresser.setInput(data, off, len);

        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try
        {
            byte[] buf = new byte[1024];
            while (!decompresser.finished())
            {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        } finally
        {
            try
            {
                o.close();
                decompresser.end();
            } catch (Exception e)
            {
            }
        }


        return output;
    }

    /**
     * 适用于mysql与客户端交互时zlib解压
     *
     * @param data   数据
     * @param off    偏移量
     * @param len    长度
     * @param srcLen 源数据长度
     * @return
     */
    public static byte[] decompress(byte[] data, int off, int len, int srcLen)
    {
        byte[] output = null;
        Inflater decompresser = new Inflater();
        decompresser.reset();
//		decompresser.setInput(data);
        decompresser.setInput(data, off, len);

        ByteArrayOutputStream o = new ByteArrayOutputStream(srcLen);
        try
        {
            o.reset();
            byte[] buf = new byte[1024];
            while (!decompresser.finished())
            {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        } finally
        {
            try
            {
                o.close();
                decompresser.end();
            } catch (Exception e)
            {
            }
        }


        return output;
    }

}
