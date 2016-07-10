

package io.mycat.memory.unsafe.storage;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Intercepts write calls and tracks total time spent writing in order to update shuffle write
 * metrics. Not thread safe.
 */
public final class TimeTrackingOutputStream extends OutputStream {

  /**private final ShuffleWriteMetrics writeMetrics;*/
  private final OutputStream outputStream;

  public TimeTrackingOutputStream(OutputStream outputStream) {
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    final long startTime = System.nanoTime();
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    final long startTime = System.nanoTime();
    outputStream.close();
  }
}
