package io.mycat.memory.unsafe.memory.mm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zagnix on 2016/6/6.
 */
public class ResultSetMemoryPool extends MemoryPool {
    private static final Logger LOG = LoggerFactory.getLogger(ResultSetMemoryPool.class);

    private  MemoryMode memoryMode ;

    /**
     * @param lock a [[MemoryManager]] instance to synchronize on
     * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
     */
    public ResultSetMemoryPool(Object lock, MemoryMode memoryMode) {
        super(lock);
        this.memoryMode = memoryMode;
    }


    private String poolName(){

        switch (memoryMode){
            case ON_HEAP:
                return  "on-heap memory";
            case OFF_HEAP:
                return "off-heap memory";
        }

        return "off-heap memory";
    }

    public ConcurrentHashMap<Long, Long> getMemoryForConnection() {
        return memoryForConnection;
    }
    /**
     * Map from taskAttemptId -> memory consumption in bytes
     */
    private ConcurrentHashMap<Long,Long> memoryForConnection = new ConcurrentHashMap<Long,Long>();

    @Override
    protected long memoryUsed() {
        synchronized (lock) {
            long used =0;
            for (Map.Entry<Long, Long> entry : memoryForConnection.entrySet()) {
                used += entry.getValue();
            }
            return used;
        }
    }


    /**
     * Returns the memory consumption, in bytes, for the given task.
     */
    public  long getMemoryUsageConnection(long taskAttemptId) {
        synchronized (lock) {
            if (!memoryForConnection.containsKey(taskAttemptId)) {
                memoryForConnection.put(taskAttemptId, 0L);
            }
            return memoryForConnection.get(taskAttemptId);
        }
    }


    /**
     * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
     * obtained, or 0 if none can be allocated.
     *
     * This call may block until there is enough free memory in some situations, to make sure each
     * task has a chance to ramp up to at least 1 / 8N of the total memory pool (where N is the # of
     * active tasks) before it is forced to spill. This can happen if the number of tasks increase
     * but an older task had a lot of memory already.
     *
     * @param numBytes number of bytes to acquire
     * @param connAttemptId the task attempt acquiring memory
     * @return the number of bytes granted to the task.
     */
    public  long acquireMemory(long numBytes, long connAttemptId) throws InterruptedException {

        synchronized (lock) {
            assert (numBytes > 0);
            // Add this connection to the taskMemory map just so we can keep an accurate count of the number
            // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
            if (!memoryForConnection.containsKey(connAttemptId)) {
                memoryForConnection.put(connAttemptId, 0L);
                // This will later cause waiting tasks to wake up and check numTasks again
                lock.notifyAll();
            }


            while (true) {
                long numActiveConns = memoryForConnection.size();
                long curMem = memoryForConnection.get(connAttemptId);

                long maxPoolSize = poolSize();
                long maxMemoryPerTask = maxPoolSize / numActiveConns;
                long minMemoryPerTask = poolSize() / (8 * numActiveConns);

                // How much we can grant this connection; keep its share within 0 <= X <= 1 / numActiveConns
                long maxToGrant = Math.min(numBytes, Math.max(0, maxMemoryPerTask - curMem));
                // Only give it as much memory as is free, which might be none if it reached 1 / numActiveConns
                long toGrant = Math.min(maxToGrant, memoryFree());

                // We want to let each connection get at least 1 / (8 * numActiveConns) before blocking;
                // if we can't give it this much now, wait for other tasks to free up memory
                // (this happens if older tasks allocated lots of memory before N grew)
                if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
                    LOG.info("Thread " + connAttemptId + " waiting for at least 1/8N of " + poolName() + " pool to be free");
                    lock.wait();
                } else {
                    long temp = memoryForConnection.get(connAttemptId);
                    memoryForConnection.put(connAttemptId, (temp + toGrant));
                    return toGrant;
                }
            }
        }
    }

    /**
     * Release `numBytes` of memory acquired by the given task.
     */
    public  void releaseMemory(long numBytes, long connAttemptId) {

        synchronized (lock) {
            long curMem = memoryForConnection.get(connAttemptId);

            long memoryToFree = 0L;

            if (curMem < numBytes) {
                System.out.print(
                        "Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
                                "of memory from the " + poolName() + "  pool");
                memoryToFree = curMem;
            } else {
                memoryToFree = numBytes;
            }

            if (memoryForConnection.containsKey(connAttemptId)) {
                long temp = memoryForConnection.get(connAttemptId);
                memoryForConnection.put(connAttemptId, (temp - memoryToFree));
                if (memoryForConnection.get(connAttemptId) <= 0) {
                    memoryForConnection.remove(connAttemptId);
                }
            }
            // Notify waiters in acquireMemory() that memory has been freed
            lock.notifyAll();
        }
    }

    /**
     * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
     * @return the number of bytes freed.
     */
    public  long releaseAllMemoryForeConnection(long connAttemptId) {
        synchronized (lock){
            long numBytesToFree = getMemoryUsageConnection(connAttemptId);
            releaseMemory(numBytesToFree,connAttemptId);
            return numBytesToFree;
        }
    }
}
