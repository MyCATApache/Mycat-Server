package io.mycat.memory.unsafe.memory.mm;


import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.array.ByteArrayMethods;
import io.mycat.memory.unsafe.memory.MemoryAllocator;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.ConcurrentHashMap;

public abstract class MemoryManager {

  private MycatPropertyConf conf;

  @GuardedBy("this")
  protected ResultSetMemoryPool onHeapExecutionMemoryPool =
          new ResultSetMemoryPool(this, MemoryMode.ON_HEAP);

  @GuardedBy("this")
  protected ResultSetMemoryPool offHeapExecutionMemoryPool =
          new ResultSetMemoryPool(this, MemoryMode.OFF_HEAP);

  protected long maxOffHeapMemory = 0L;
  protected long offHeapExecutionMemory = 0L;
  private  int numCores = 0;

  public MemoryManager(MycatPropertyConf conf, int numCores, long onHeapExecutionMemory){
    this.conf = conf;
    this.numCores =numCores;
    maxOffHeapMemory = conf.getSizeAsBytes("mycat.memory.offHeap.size","128m");
    offHeapExecutionMemory = maxOffHeapMemory;
    onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory);

    offHeapExecutionMemoryPool.incrementPoolSize(offHeapExecutionMemory);
  }

  protected abstract long acquireExecutionMemory(long numBytes,long taskAttemptId,MemoryMode memoryMode) throws InterruptedException;

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
public void releaseExecutionMemory(long numBytes, long taskAttemptId, MemoryMode memoryMode) {
  synchronized (this) {
      switch (memoryMode) {
          case ON_HEAP:
              onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
              break;
          case OFF_HEAP:
              offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId);
              break;
      }
   }

  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  public  long releaseAllExecutionMemoryForConnection(long connAttemptId){
      synchronized(this) {
          return (onHeapExecutionMemoryPool.releaseAllMemoryForeConnection(connAttemptId) +
                  offHeapExecutionMemoryPool.releaseAllMemoryForeConnection(connAttemptId));
      }
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  public  final long executionMemoryUsed() {
      synchronized(this) {
          return (onHeapExecutionMemoryPool.memoryUsed() + offHeapExecutionMemoryPool.memoryUsed());
      }
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  public  long getExecutionMemoryUsageForConnection(long connAttemptId)  {
      synchronized (this) {
          assert (connAttemptId >= 0);
          return (onHeapExecutionMemoryPool.getMemoryUsageConnection(connAttemptId) +
                  offHeapExecutionMemoryPool.getMemoryUsageConnection(connAttemptId));
      }
  }

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  public final MemoryMode tungstenMemoryMode(){
    if (conf.getBoolean("mycat.memory.offHeap.enabled", false)) {
      assert (conf.getSizeAsBytes("mycat.memory.offHeap.size",0) > 0);
      assert (Platform.unaligned());
      return MemoryMode.OFF_HEAP;
    } else {
      return  MemoryMode.ON_HEAP;
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "mycat.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   */
  public long pageSizeBytes() {

    long minPageSize = 1L * 1024 * 1024 ;  // 1MB
    long maxPageSize = 64L * minPageSize ; // 64MB

    int cores = 0;

    if (numCores > 0){
       cores = numCores ;
    } else {
      cores =  Runtime.getRuntime().availableProcessors();
    }

    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    int safetyFactor = 16;
    long maxTungstenMemory = 0L;

    switch (tungstenMemoryMode()){
      case ON_HEAP:
        maxTungstenMemory = onHeapExecutionMemoryPool.poolSize();
        break;
      case OFF_HEAP:
        maxTungstenMemory = offHeapExecutionMemoryPool.poolSize();
        break;
    }

    long size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor);
    long defaultSize =  Math.min(maxPageSize, Math.max(minPageSize, size));
    defaultSize = conf.getSizeAsBytes("mycat.buffer.pageSize", defaultSize);

    return defaultSize;
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  public final MemoryAllocator tungstenMemoryAllocator() {
    switch (tungstenMemoryMode()){
      case ON_HEAP:
        return MemoryAllocator.HEAP;
      case OFF_HEAP:
        return MemoryAllocator.UNSAFE;
    }
    return null;
  }

    /**
     * Get Direct Memory Usage.
     */
    public final ConcurrentHashMap<Long, Long> getDirectMemorUsage() {

        return offHeapExecutionMemoryPool.getMemoryForConnection();
    }
}
