package org.opencloudb.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.util.TimeUtil;

public class AIOSocketWR extends SocketWR {
	private static final AIOReadHandler aioReadHandler = new AIOReadHandler();
	//private static final AIOWriteHandler aioWriteHandler = new AIOWriteHandler();
	private final AsynchronousSocketChannel channel;
	protected final AbstractConnection con;
	private final AtomicBoolean writing = new AtomicBoolean(false);

	public AIOSocketWR(AbstractConnection conn) {
		channel = (AsynchronousSocketChannel) conn.getChannel();
		this.con = conn;
	}

	@Override
	public void asynRead() {

		ByteBuffer theBuffer = con.readBuffer;
		if (theBuffer == null) {
			theBuffer = con.processor.getBufferPool().allocate();
			con.readBuffer = theBuffer;
			channel.read(theBuffer, this, aioReadHandler);

		} else if (theBuffer.hasRemaining()) {
			channel.read(theBuffer, this, aioReadHandler);
		} else {
			throw new java.lang.IllegalArgumentException("full buffer to read ");
		}

	}

	private void asynWrite(ByteBuffer buffer) throws IOException
    {
		buffer.flip();
      int write=     flushChannel(channel,buffer,30);
        onWriteFinished( write);
		//this.channel.write(buffer, this, aioWriteHandler);
	}

	public static int flushChannel(final AsynchronousSocketChannel channel,
									final ByteBuffer bb, final long writeTimeout) throws IOException {

		if (!bb.hasRemaining()) {
			return 0;
		}
		int nWrite = bb.limit();
		try {
			while (bb.hasRemaining()) {
				channel.write(bb).get(writeTimeout, TimeUnit.SECONDS);
			}
		} catch (Exception ie) {
			throw new IOException(ie);
		}
		return nWrite;
	}


	/**
	 * return true ,means no more data
	 * 
	 * @return
	 */
	private boolean write0() throws IOException
    {
		ByteBuffer theBuffer = con.writeBuffer;
		if (theBuffer == null || !theBuffer.hasRemaining()) {// writeFinished,但要区分bufer是否NULL，不NULL，要回收
			if (theBuffer != null) {
				con.recycle(theBuffer);
				con.writeBuffer = null;

			}
			// poll again
			ByteBuffer buffer = con.writeQueue.poll();
			// more data
			if (buffer != null) {
				if (buffer.limit() == 0) {
					con.recycle(buffer);
					con.writeBuffer = null;
					con.close("quit cmd");
					return true;
				} else {
					con.writeBuffer = buffer;
					asynWrite(buffer);
					return false;
				}
			} else {
				// no buffer
				writing.set(false);
				return true;
			}
		} else {
			theBuffer.compact();
			asynWrite(theBuffer);
			return false;
		}

	}

	protected void onWriteFinished(int result) throws IOException
    {
		con.netOutBytes += result;
		con.processor.addNetOutBytes(result);
		con.lastWriteTime = TimeUtil.currentTimeMillis();
		boolean noMoreData = this.write0();
		if (noMoreData) {
			this.doNextWriteCheck();
		}

	}

	public void doNextWriteCheck() {
		if (!writing.compareAndSet(false, true)) {
			return;
		}
        boolean noMoreData = false;
        try
        {
            noMoreData = this.write0();
            if (noMoreData) {
                if (!con.writeQueue.isEmpty()) {
                    this.write0();
                }

            }
        } catch (IOException e)
        {
           throw new RuntimeException(e);
        }


	}
}

//class AIOWriteHandler implements CompletionHandler<Integer, AIOSocketWR> {
//
//	@Override
//	public void completed(final Integer result, final AIOSocketWR wr) {
//		try {
//			if (result >= 0) {
//				wr.onWriteFinished(result);
//			} else {
//				wr.con.close("write erro " + result);
//			}
//		} catch (Exception e) {
//			AbstractConnection.LOGGER.warn("caught aio process err:", e);
//		}
//
//	}
//
//	@Override
//	public void failed(Throwable exc, AIOSocketWR wr) {
//		wr.con.close("write failed " + exc);
//	}
//
//}

class AIOReadHandler implements CompletionHandler<Integer, AIOSocketWR> {
	@Override
	public void completed(final Integer i, final AIOSocketWR wr) {
		// con.getProcessor().getExecutor().execute(new Runnable() {
		// public void run() {
		if (i > 0) {
			try {
				wr.con.onReadData(i);
				wr.con.asynRead();
			} catch (IOException e) {
				wr.con.close("handle err:" + e);
			}
		} else if (i == -1) {
			// System.out.println("read -1 xxxxxxxxx "+con);
			wr.con.close("client closed");
		}
		// }
		// });
	}

	@Override
	public void failed(Throwable exc, AIOSocketWR wr) {
		wr.con.close(exc.toString());

	}
}
