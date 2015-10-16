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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.Deflater;
import java.util.zip.Inflater;


public class CompressUtil
{

    public int MINI_LENGTH_TO_COMPRESS = 50;

    public static ByteBuffer compressMysqlPacket(ByteBuffer input, AbstractConnection con,ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue)
    {

        byte[] byteArrayFromBuffer = getByteArrayFromBuffer(input);
        con.recycle(input);
        byteArrayFromBuffer = mergeBytes(byteArrayFromBuffer, compressUnfinishedDataQueue);
        return compressMysqlPacket(byteArrayFromBuffer, con,compressUnfinishedDataQueue);
    }

    private static ByteBuffer compressMysqlPacket(byte[] data, AbstractConnection con,ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue)
    {

        ByteBuffer byteBuf = con.allocate();
        byteBuf = con.checkWriteBuffer(byteBuf, data.length, false);
        MySQLMessage msg = new MySQLMessage(data);
        while (msg.hasRemaining())
        {
            int i1 = msg.length() - msg.position();
            int i = 0;
            if (i1 > 3)
            {
                i = msg.readUB3();
                msg.move(-3);
            }
            if (i1 < i + 4)
            {
                byte[] e = msg.readBytes(i1);
                if (e.length != 0)
                {
                    compressUnfinishedDataQueue.add(e);
                    //throw new RuntimeException("不完整的包");
                }
            } else
            {
                byte[] e = msg.readBytes(i + 4);
                if (e.length != 0)
                {

                    if (e.length <= 54)
                    {
                        BufferUtil.writeUB3(byteBuf, e.length);
                        byteBuf.put(e[3]);
                        BufferUtil.writeUB3(byteBuf, 0);
                        byteBuf.put(e);

                    } else
                    {
                        byte[] compress = compress(e);
                        BufferUtil.writeUB3(byteBuf, compress.length);
                        byteBuf.put(e[3]);
                        BufferUtil.writeUB3(byteBuf, e.length);
                        byteBuf.put(compress);
                    }

                }
            }
        }


        return byteBuf;
    }

    public static List<byte[]> decompressMysqlPacket(byte[] data, ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue)
    {
        MySQLMessage mm = new MySQLMessage(data);
        int len = mm.readUB3();
        byte packetId = mm.read();
        int oldLen = mm.readUB3();
        if (len == data.length - 4)
        {
            return Lists.newArrayList(data);
        } else if (oldLen == 0)
        {
            byte[] readBytes = mm.readBytes();
         //   return Lists.newArrayList(readBytes);
            return splitPack(readBytes, decompressUnfinishedDataQueue);
        } else
        {
            byte[] de = decompress(data, 7, data.length - 7);
            return splitPack(de, decompressUnfinishedDataQueue);
        }
    }

    private static List<byte[]> splitPack(byte[] in, ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue)
    {
        in = mergeBytes(in, decompressUnfinishedDataQueue);

        List<byte[]> rtn = new ArrayList<>();
        MySQLMessage msg = new MySQLMessage(in);
        while (msg.hasRemaining())
        {
            int i1 = msg.length() - msg.position();
            int i = 0;
            if (i1 > 3)
            {
                i = msg.readUB3();
                msg.move(-3);
            }
            if (i1 < i + 4)
            {
                byte[] e = msg.readBytes(i1);
                if (e.length != 0)
                {
                    decompressUnfinishedDataQueue.add(e);
                }
            } else
            {
                byte[] e = msg.readBytes(i + 4);
                if (e.length != 0)
                {
                    rtn.add(e);
                }
            }
        }


        return rtn;
    }

    private static byte[] mergeBytes(byte[] in, ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue)
    {
        if (!decompressUnfinishedDataQueue.isEmpty())
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try
            {
                while (!decompressUnfinishedDataQueue.isEmpty())
                {
                    outputStream.write(decompressUnfinishedDataQueue.poll());
                }
                outputStream.write(in);
                in = outputStream.toByteArray();
                outputStream.close();

            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return in;
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
        return compress(getByteArrayFromBuffer(byteBuf));
    }


    public static byte[] compress(byte[] data)
    {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        try
        {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished())
            {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        } finally
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
