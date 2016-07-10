/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mycat.memory.unsafe.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * A class for writing JVM objects directly to a file on disk. This class allows data to be appended
 * to an existing block and can guarantee atomicity in the case of faults as it allows the caller to
 * revert partial writes.
 *
 * This class does not support concurrent writes. Also, once the writer has been opened it cannot be
 * reopened again.
 */
public  class DiskRowWriter extends OutputStream {
  /** The file channel, used for repositioning / truncating the file. */
  private static final Logger LOG = LoggerFactory.getLogger(DiskRowWriter.class);

  private FileChannel channel = null;
  private OutputStream bs = null;
  private FileOutputStream fos  = null;
  private TimeTrackingOutputStream ts  = null;
  private SerializationStream objOut  = null;
  private boolean initialized = false;
  private boolean hasBeenClosed = false;
  private boolean commitAndCloseHasBeenCalled = false;

  /**
   * Cursors used to represent positions in the file.
   *
   * xxxxxxxx|--------|---       |
   *         ^        ^          ^
   *         |        |        finalPosition
   *         |      reportedPosition
   *       initialPosition
   *
   * initialPosition: Offset in the file where we start writing. Immutable.
   * reportedPosition: Position at the time of the last update to the write metrics.
   * finalPosition: Offset where we stopped writing. Set on closeAndCommit() then never changed.
   * -----: Current writes to the underlying file.
   * xxxxx: Existing contents of the file.
   */
  private long initialPosition = 0;
  private long finalPosition = -1;
  private long reportedPosition = 0;

  /**
   * Keep track of number of records written and also use this to periodically
   * output bytes written since the latter is expensive to do for each record.
   */
  private long numRecordsWritten = 0;

  private  File file;
  private SerializerInstance serializerInstance;
  private int bufferSize;
  private  OutputStream compressStream;
  private boolean syncWrites;
  // These write metrics concurrently shared with other active DiskBlockObjectWriters who
  // are themselves performing writes. All updates must be relative.
  /**ShuffleWriteMetrics  writeMetrics,*/
  private ConnectionId blockId;


  public DiskRowWriter(
          File file,
          SerializerInstance serializerInstance,
          int bufferSize,
          OutputStream compressStream ,
          boolean syncWrites,
          ConnectionId blockId) throws IOException {

    this.file = file;
    this.serializerInstance = serializerInstance;
    this.bufferSize = bufferSize;
    this.compressStream = compressStream;
    this.syncWrites = syncWrites;
    this.blockId = blockId;
    initialPosition = file.length();
    reportedPosition = initialPosition;
  }


  public DiskRowWriter open() throws FileNotFoundException {

    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.");
    }

    fos = new FileOutputStream(file,true);
    ts = new TimeTrackingOutputStream(/**writeMetrics,*/ fos);
    channel = fos.getChannel();
    bs = new BufferedOutputStream(ts,bufferSize);
    objOut = serializerInstance.serializeStream(bs);
    initialized = true;

    return this;

  }


  @Override
  public void close() {
    if (initialized) {
      try {
        if (syncWrites) {
          //Force outstanding writes to disk and track how long it takes
          objOut.flush();
          long start = System.nanoTime();
          fos.getFD().sync();
          // writeMetrics.incWriteTime(System.nanoTime() - start);
        }
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }finally {
        objOut.close();
      }
      channel = null;
      bs = null;
      fos = null;
      ts = null;
      objOut = null;
      initialized = false;
      hasBeenClosed = true;
    }
  }

  public boolean isOpen(){
    return objOut != null;
  }

  /**
   * Flush the partial writes and commit them as a single atomic block.
   */
  public void commitAndClose() throws IOException {
    if (initialized) {
      // NOTE: Because Kryo doesn’t flush the underlying stream we explicitly flush both the
      // serializer stream and the lower level stream.
      objOut.flush();
      bs.flush();
      close();
      finalPosition = file.length();
      // In certain compression codecs, more bytes are written after close() is called
      //writeMetrics.incBytesWritten(finalPosition - reportedPosition)
    } else {
      finalPosition = file.length();
    }
    commitAndCloseHasBeenCalled = true;
  }


  /**
   * Reverts writes that haven’t been flushed yet. Callers should invoke this function
   * when there are runtime exceptions. This method will not throw, though it may be
   * unsuccessful in truncating written data.
   *
   * @return the file that this DiskRowWriter wrote to.
   */
  public File revertPartialWritesAndClose() throws IOException {
    // Discard current writes. We do this by flushing the outstanding writes and then
    // truncating the file to its initial position.
    try {
      if (initialized) {
        // writeMetrics.decBytesWritten(reportedPosition - initialPosition)
        // writeMetrics.decRecordsWritten(numRecordsWritten)
        objOut.flush();
        bs.flush();
        close();
      }

      FileOutputStream truncateStream = new FileOutputStream(file, true);
      try {
        truncateStream.getChannel().truncate(initialPosition);
        return file;
      } finally {
        truncateStream.close();
      }
    } catch(Exception e) {
      LOG.error(e.getMessage());
      return file;
    }
  }

  /**
   * Writes a key-value pair.
   */
  private void write(Object key, Object value) throws IOException {
    if (!initialized) {
      open();
    }

    objOut.writeKey(key);
    objOut.writeValue(value);
    recordWritten();
  }
  @Override
  public void write(int b){
    throw new UnsupportedOperationException();
  }
  @Override
  public void write(byte [] kvBytes ,int offs, int len) throws IOException {
    if (!initialized) {
      open();
    }

    bs.write(kvBytes,offs, len);
  }

  /**
   * Notify the writer that a record worth of bytes has been written with OutputStream#write.
   */
  public void recordWritten() throws IOException {
    numRecordsWritten += 1;
//writeMetrics.incRecordsWritten(1)

// TODO: call updateBytesWritten() less frequently.
    if (numRecordsWritten % 32 == 0) {
      updateBytesWritten();
    }
  }

  /**
   * Report the number of bytes written in this writer’s shuffle write metrics.
   * Note that this is only valid before the underlying streams are closed.
   */
  private void updateBytesWritten() throws IOException {
    long pos = channel.position();
    //writeMetrics.incBytesWritten(pos - reportedPosition)
    reportedPosition = pos;
  }

  @Override
  public void flush() throws IOException {
    objOut.flush();
    bs.flush();
  }
}
