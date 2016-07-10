package io.mycat.memory.unsafe.memory.mm;

import com.google.common.annotations.VisibleForTesting;

import io.mycat.memory.unsafe.memory.MemoryBlock;
import io.mycat.memory.unsafe.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;

/** 
 * Modify by zagnix 
 * Manages the memory allocated by an individual thread.
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * 2^32 * 8 bytes, which is
 * approximately 35 terabytes of memory.
 */
public class DataNodeMemoryManager {

  private final Logger logger = LoggerFactory.getLogger(DataNodeMemoryManager.class);

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of bits used to encode offsets in data pages. */
  public static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's
   * maximum page size is limited by the maximum amount of data that can be stored in a long[]
   * array, which is (2^32 - 1) * 8 bytes (or 16 gigabytes). Therefore, we cap this at 16 gigabytes.
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** Bit mask for the lower 51 bits of a long. */
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long */
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private final MemoryManager memoryManager;

  private final long connectionAttemptId;

  /**
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   */
  public final MemoryMode tungstenMemoryMode;

  /**
   * Tracks spillable memory consumers.
   */
  @GuardedBy("this")
  private final HashSet<MemoryConsumer> consumers;

  /**
   * The amount of memory that is acquired but not used.
   */
  private volatile long acquiredButNotUsed = 0L;

  /**
   * Construct a new DataNodeMemoryManager.
   */
  public DataNodeMemoryManager(MemoryManager memoryManager, long connectionAttemptId) {
    this.tungstenMemoryMode = memoryManager.tungstenMemoryMode();
    this.memoryManager = memoryManager;
    this.connectionAttemptId = connectionAttemptId;
    this.consumers = new HashSet<MemoryConsumer>();
  }

  /**
   * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
   * spill() of consumers to release more memory.
   *
   * @return number of bytes successfully granted (<= N).
   */
  public long acquireExecutionMemory(long required,MemoryMode mode,MemoryConsumer consumer) throws InterruptedException {

    assert(required >= 0);
    // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
    // memory here, then it may not make sense to spill since that would only end up freeing
    // off-heap memory. This is subject to change, though, so it may be risky to make this
    // optimization now in case we forget to undo it late when making changes.
    synchronized (this) {
      long got = memoryManager.acquireExecutionMemory(required,connectionAttemptId, mode);
      // Try to release memory from other consumers first, then we can reduce the frequency of
      // spilling, avoid to have too many spilled files.
      if (got < required) {
        // Call spill() on other consumers to release memory
        for (MemoryConsumer c: consumers) {
          if (c != consumer && c.getUsed() > 0) {
            try {
               /**
               * 调用spill函数，写数据到磁盘中
               */
              long released = c.spill(required - got, consumer);
                if (released > 0 && mode == tungstenMemoryMode) {
                logger.info("Thread "+connectionAttemptId+" released "+ JavaUtils.bytesToString(released) +
                        " from "+ c +" for" + consumer);
                got += memoryManager.acquireExecutionMemory(required - got, connectionAttemptId, mode);
                if (got >= required) {
                  break;
                }
              }
            } catch (IOException e) {
              logger.error("error while calling spill() on " + c, e);
              throw new OutOfMemoryError("error while calling spill() on " + c + " : "
                + e.getMessage());
            }
          }
        }
      }

      // call spill() on itself
      if (got < required && consumer != null) {
        try {
          long released = consumer.spill(required - got, consumer);
          if (released > 0 && mode == tungstenMemoryMode) {
            logger.info("Thread " + connectionAttemptId +
                    " released "+   JavaUtils.bytesToString(released) +"from itself ("+consumer+ ")");
            got += memoryManager.acquireExecutionMemory(required - got, connectionAttemptId, mode);
          }
        } catch (IOException e) {
          logger.error("error while calling spill() on " + consumer, e);
          throw new OutOfMemoryError("error while calling spill() on " + consumer + " : "
            + e.getMessage());

        }
      }

      if (consumer != null) {
        consumers.add(consumer);
      }
     // logger.info("Thread" + connectionAttemptId + " acquire "+  JavaUtils.bytesToString(got) +" for "+ consumer+"");
      return got;
    }
  }

  /**
   * Release N bytes of execution memory for a MemoryConsumer.
   */
  public void releaseExecutionMemory(long size, MemoryMode mode, MemoryConsumer consumer) {
    logger.debug   ("Thread" + connectionAttemptId + " release "+  JavaUtils.bytesToString(size) +" from "+ consumer+"");

    memoryManager.releaseExecutionMemory(size, connectionAttemptId, mode);
  }

  /**
   * Dump the memory usage of all consumers.
   */
  public void showMemoryUsage() {
    logger.info("Memory used in Thread " + connectionAttemptId);
    synchronized (this) {
      long memoryAccountedForByConsumers = 0;
      for (MemoryConsumer c: consumers) {
        long totalMemUsage = c.getUsed();
        memoryAccountedForByConsumers += totalMemUsage;
        if (totalMemUsage > 0) {
          logger.info("Acquired by " + c + ": " + JavaUtils.bytesToString(totalMemUsage));
        }
      }
      long memoryNotAccountedFor =
        memoryManager.getExecutionMemoryUsageForConnection(connectionAttemptId) - memoryAccountedForByConsumers;
      logger.info(
        "{} bytes of memory were used by task {} but are not associated with specific consumers",
        memoryNotAccountedFor, connectionAttemptId);
      logger.info(
        "{} bytes of memory are used for execution and {} bytes of memory are used for storage",
        memoryManager.executionMemoryUsed());
    }
  }

  /**
   * Return the page size in bytes.
   */
  public long pageSizeBytes() {
    return memoryManager.pageSizeBytes();
  }

  /**
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of Tungsten memory that will be shared between operators.
   *
   * Returns `null` if there was not enough memory to allocate the page. May return a page that
   * contains fewer bytes than requested, so callers should verify the size of returned pages.
   */
  public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException(
        "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
    }

    /**
     * 这里spill到磁盘中，释放内存空间
     */
    long acquired = 0;
    try {
      acquired = acquireExecutionMemory(size,tungstenMemoryMode, consumer);
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    }

    if (acquired <= 0) {
      return null;
    }

    final int pageNumber;

    synchronized (this) {
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        releaseExecutionMemory(acquired, tungstenMemoryMode, consumer);
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      allocatedPages.set(pageNumber);
    }



    MemoryBlock page = null;

    try {
      page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
    } catch (OutOfMemoryError e) {
      logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
      // there is no enough memory actually, it means the actual free memory is smaller than
      // MemoryManager thought, we should keep the acquired memory.
      synchronized (this) {
        acquiredButNotUsed += acquired;
        allocatedPages.clear(pageNumber);
      }
      // this could trigger spilling to free some pages.
      return allocatePage(size, consumer);
    }

    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;

//    logger.info("Allocate page number " + pageNumber + " ("+ acquired +" bytes)");

    return page;
  }

  /**
   * Free a block of memory allocated via {@link DataNodeMemoryManager#allocatePage}.
   */
  public void freePage(MemoryBlock page, MemoryConsumer consumer) {

    assert (page.pageNumber != -1) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert(allocatedPages.get(page.pageNumber));
    pageTable[page.pageNumber] = null;

    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }

    logger.trace("Freed page number "+ page.pageNumber +" ("+page.size()  +" bytes)");

    long pageSize = page.size();
    memoryManager.tungstenMemoryAllocator().free(page);
    releaseExecutionMemory(pageSize,tungstenMemoryMode,consumer);
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   *
   * @param page a data page allocated by {@link DataNodeMemoryManager#allocatePage}/
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {

    if (tungstenMemoryMode == MemoryMode.OFF_HEAP) {
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      offsetInPage -= page.getBaseOffset();
    }

    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }

  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }

  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    return (int) (pagePlusOffsetAddress >>> OFFSET_BITS);
  }

  private static long decodeOffset(long pagePlusOffsetAddress) {
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

  /**
   * Get the page associated with an address encoded by
   * {@link DataNodeMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      assert (page.getBaseObject() != null);
      return page.getBaseObject();
    } else {
      return null;
    }
  }

  /**
   * Get the offset associated with an address encoded by
   * {@link DataNodeMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (tungstenMemoryMode == MemoryMode.ON_HEAP) {
      return offsetInPage;
    } else {
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   */
  public long cleanUpAllAllocatedMemory() {
    synchronized (this) {
      for (MemoryConsumer c: consumers) {
        if (c != null && c.getUsed() > 0) {
          // In case of failed task, it's normal to see leaked memory
          logger.warn("leak " + JavaUtils.bytesToString(c.getUsed()) + " memory from " + c);
        }
      }
      consumers.clear();

      for (MemoryBlock page : pageTable) {
        if (page != null) {
          logger.warn("leak a page: " + page + " in task " + connectionAttemptId);
          memoryManager.tungstenMemoryAllocator().free(page);
        }
      }
      Arrays.fill(pageTable, null);
    }

    // release the memory that is not used by any consumer.
    memoryManager.releaseExecutionMemory(acquiredButNotUsed, connectionAttemptId, tungstenMemoryMode);

    return memoryManager.releaseAllExecutionMemoryForConnection(connectionAttemptId);
  }

  /**
   * Returns the memory consumption, in bytes, for the current task.
   */
  public long getMemoryConsumptionForThisConnection() {
    return memoryManager.getExecutionMemoryUsageForConnection(connectionAttemptId);
  }

  /**
   * Returns Tungsten memory mode
   */
  public MemoryMode getTungstenMemoryMode() {
    return tungstenMemoryMode;
  }
}
