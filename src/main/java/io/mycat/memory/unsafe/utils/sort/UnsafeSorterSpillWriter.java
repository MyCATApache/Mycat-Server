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

package io.mycat.memory.unsafe.utils.sort;



import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.storage.*;

import java.io.File;
import java.io.IOException;

/**
 * Spills a list of sorted records to disk. Spill files have the following format:
 *
 *   [# of records (int)] [[len (int)][prefix (long)][data (bytes)]...]
 */
public final class UnsafeSorterSpillWriter {

  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  // Small writes to DiskRowWriter will be fairly inefficient. Since there doesn't seem to
  // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
  // data through a byte array.
  private byte[] writeBuffer = new byte[DISK_WRITE_BUFFER_SIZE];

  private final File file;
  private final ConnectionId conId;
  private final int numRecordsToWrite;
  private DiskRowWriter writer;
  private DataNodeFileManager diskBlockManager;
  private int numRecordsSpilled = 0;

  public UnsafeSorterSpillWriter(
      DataNodeDiskManager blockManager,
      int fileBufferSize,
      int numRecordsToWrite) throws IOException {

    this.diskBlockManager =  blockManager.diskBlockManager();
    this.conId =  diskBlockManager.createTempLocalBlock();
    this.file = diskBlockManager.getFile(this.conId);

    this.numRecordsToWrite = numRecordsToWrite;
    // Unfortunately, we need a serializer instance in order to construct a DiskRowWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskRowWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    writer = blockManager.getDiskWriter(conId, file, DummySerializerInstance.INSTANCE, fileBufferSize/**,writeMetrics*/);
    // Write the number of records
    writeIntToBuffer(numRecordsToWrite, 0);
    writer.write(writeBuffer, 0, 4);
  }

  // Based on DataOutputStream.writeLong.
  private void writeLongToBuffer(long v, int offset) throws IOException {
    writeBuffer[offset + 0] = (byte)(v >>> 56);
    writeBuffer[offset + 1] = (byte)(v >>> 48);
    writeBuffer[offset + 2] = (byte)(v >>> 40);
    writeBuffer[offset + 3] = (byte)(v >>> 32);
    writeBuffer[offset + 4] = (byte)(v >>> 24);
    writeBuffer[offset + 5] = (byte)(v >>> 16);
    writeBuffer[offset + 6] = (byte)(v >>>  8);
    writeBuffer[offset + 7] = (byte)(v >>>  0);
  }

  // Based on DataOutputStream.writeInt.
  private void writeIntToBuffer(int v, int offset) throws IOException {
    writeBuffer[offset + 0] = (byte)(v >>> 24);
    writeBuffer[offset + 1] = (byte)(v >>> 16);
    writeBuffer[offset + 2] = (byte)(v >>>  8);
    writeBuffer[offset + 3] = (byte)(v >>>  0);
  }

  /**
   * Write a record to a spill file.
   *
   * @param baseObject the base object / memory page containing the record
   * @param baseOffset the base offset which points directly to the record data.
   * @param recordLength the length of the record.
   * @param keyPrefix a sort key prefix
   */
  public void write(
      Object baseObject,
      long baseOffset,
      int recordLength,
      long keyPrefix) throws IOException {
    if (numRecordsSpilled == numRecordsToWrite) {
      throw new IllegalStateException(
        "Number of records written exceeded numRecordsToWrite = " + numRecordsToWrite);
    } else {
      numRecordsSpilled++;
    }

    /**
     * [# of records (int)] [[len (int)][prefix (long)][data (bytes)]...]
     * 一条记录在文件中格式
     * */

      /**
       * recordLength记录长度 4个bytes
       */
    writeIntToBuffer(recordLength, 0);
    /**
     * 排序key，8个bytes
     */
    writeLongToBuffer(keyPrefix, 4);
    /**
     * dataRemaining要写的真实数据长度bytes
     */
    int dataRemaining = recordLength;
    /**
     * 写buffer剩余的空间
     */
    int freeSpaceInWriteBuffer = DISK_WRITE_BUFFER_SIZE - 4 - 8; // space used by prefix + len

    /**
     *记录在内存中的地址偏移量
     */
    long recordReadPosition = baseOffset;

    while (dataRemaining > 0) {
      /**
       * 计算本次需要从内存中读取的实际数据，取freeSpaceInWriteBuffer和dataRemaining
       * 中的最小值
       */
      final int toTransfer = Math.min(freeSpaceInWriteBuffer, dataRemaining);

        /**
         * 执行数据拷贝动作，将baseObject的数据拷贝到writeBuffer中
         */
      Platform.copyMemory(
        baseObject,/**srd*/
        recordReadPosition,/**offset*/
        writeBuffer, /**write dst*/
        Platform.BYTE_ARRAY_OFFSET + (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer),/**write offset*/
        toTransfer);

      /**
       * 将writeBuffer中数据写到磁盘中
       */
      writer.write(writeBuffer, 0, (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer) + toTransfer);
      /**
       * 读指针移动toTransfer实际写的数据大小
       */
      recordReadPosition += toTransfer;
      /**
       * record还剩下多少数据要写入磁盘中
       */
      dataRemaining -= toTransfer;
      /**
       * 本次WriteBuffer初始化大小初始化为DISK_WRITE_BUFFER_SIZE
       */
      freeSpaceInWriteBuffer = DISK_WRITE_BUFFER_SIZE;
    }

      /**
       * 写剩余数据到磁盘中
       */
    if (freeSpaceInWriteBuffer < DISK_WRITE_BUFFER_SIZE) {

      writer.write(writeBuffer, 0, (DISK_WRITE_BUFFER_SIZE - freeSpaceInWriteBuffer));

    }

    /**
     * writer类中数据统计
     */
    writer.recordWritten();
  }

  public void close() throws IOException {
    writer.commitAndClose();
    writer = null;
    writeBuffer = null;
  }

  public File getFile() {
    return file;
  }

  public UnsafeSorterSpillReader getReader(SerializerManager serializerManager) throws IOException {
    return new UnsafeSorterSpillReader(serializerManager, file, conId);
  }
}
