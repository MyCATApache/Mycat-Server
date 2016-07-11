package io.mycat.memory.unsafe.memory.mm;


import io.mycat.memory.unsafe.utils.MycatPropertyConf;

/**
 * Created by zagnix on 2016/6/7.
 */
public class ResultMergeMemoryManager extends MemoryManager {

    private long  maxOnHeapExecutionMemory;
    private int numCores;
    private MycatPropertyConf conf;
    public ResultMergeMemoryManager(MycatPropertyConf conf, int numCores, long onHeapExecutionMemory){
        super(conf,numCores,onHeapExecutionMemory);
        this.conf = conf;
        this.numCores = numCores;
        this.maxOnHeapExecutionMemory = onHeapExecutionMemory;
    }

    @Override
    protected  synchronized long acquireExecutionMemory(long numBytes,long taskAttemptId,MemoryMode memoryMode) throws InterruptedException {
        switch (memoryMode) {
            case ON_HEAP:
                return  onHeapExecutionMemoryPool.acquireMemory(numBytes,taskAttemptId);
            case OFF_HEAP:
                return  offHeapExecutionMemoryPool.acquireMemory(numBytes,taskAttemptId);
        }
        return 0L;
    }

}
