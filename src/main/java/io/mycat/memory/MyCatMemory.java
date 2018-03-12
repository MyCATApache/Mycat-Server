package io.mycat.memory;


import com.google.common.annotations.VisibleForTesting;
import io.mycat.config.model.SystemConfig;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryManager;
import io.mycat.memory.unsafe.memory.mm.ResultMergeMemoryManager;
import io.mycat.memory.unsafe.storage.DataNodeDiskManager;
import io.mycat.memory.unsafe.storage.SerializerManager;
import io.mycat.memory.unsafe.utils.JavaUtils;
import io.mycat.memory.unsafe.utils.MycatPropertyConf;
import org.apache.log4j.Logger;

/**
 * Created by zagnix on 2016/6/2.
 * Mycat内存管理工具类
 * 规划为三部分内存:结果集处理内存,系统预留内存,网络处理内存
 * 其中网络处理内存部分全部为Direct Memory
 * 结果集内存分为Direct Memory 和 Heap Memory，但目前仅使用Direct Memory
 * 系统预留内存为 Heap Memory。
 * 系统运行时，必须设置-XX:MaxDirectMemorySize 和 -Xmx JVM参数
 * -Xmx1024m -Xmn512m -XX:MaxDirectMemorySize=2048m -Xss256K -XX:+UseParallelGC
 */

public class MyCatMemory {
	private static Logger LOGGER = Logger.getLogger(MyCatMemory.class);

	public final  static double DIRECT_SAFETY_FRACTION  = 0.7;
	private final long systemReserveBufferSize;

	private final long memoryPageSize;
	private final long spillsFileBufferSize;
	private final long resultSetBufferSize;
	private final int numCores;


	/**
	 * 内存管理相关关键类
	 */
	private final MycatPropertyConf conf;
	private final MemoryManager resultMergeMemoryManager;
	private final DataNodeDiskManager blockManager;
	private final SerializerManager serializerManager;
	private final SystemConfig system;


	public MyCatMemory(SystemConfig system,long totalNetWorkBufferSize) throws NoSuchFieldException, IllegalAccessException {

		this.system = system;

		LOGGER.info("useOffHeapForMerge = " + system.getUseOffHeapForMerge());
		LOGGER.info("memoryPageSize = " + system.getMemoryPageSize());
		LOGGER.info("spillsFileBufferSize = " + system.getSpillsFileBufferSize());
		LOGGER.info("useStreamOutput = " + system.getUseStreamOutput());
		LOGGER.info("systemReserveMemorySize = " + system.getSystemReserveMemorySize());
		LOGGER.info("totalNetWorkBufferSize = " + JavaUtils.bytesToString2(totalNetWorkBufferSize));
		LOGGER.info("dataNodeSortedTempDir = " + system.getDataNodeSortedTempDir());

		this.conf = new MycatPropertyConf();
		numCores = Runtime.getRuntime().availableProcessors();

		this.systemReserveBufferSize = JavaUtils.
				byteStringAsBytes(system.getSystemReserveMemorySize());
		this.memoryPageSize = JavaUtils.
				byteStringAsBytes(system.getMemoryPageSize());

		this.spillsFileBufferSize = JavaUtils.
				byteStringAsBytes(system.getSpillsFileBufferSize());

		/**
		 * 目前merge，order by ，limit 没有使用On Heap内存
		 */
		long maxOnHeapMemory =  (Platform.getMaxHeapMemory()-systemReserveBufferSize);

		assert maxOnHeapMemory > 0;

		resultSetBufferSize =
				(long)((Platform.getMaxDirectMemory()-2*totalNetWorkBufferSize)*DIRECT_SAFETY_FRACTION);

		assert resultSetBufferSize > 0;

		/**
		 * mycat.merge.memory.offHeap.enabled
		 * mycat.buffer.pageSize
		 * mycat.memory.offHeap.size
		 * mycat.merge.file.buffer
		 * mycat.direct.output.result
		 * mycat.local.dir
		 */

		if(system.getUseOffHeapForMerge()== 1){
			conf.set("mycat.memory.offHeap.enabled","true");
		}else{
			conf.set("mycat.memory.offHeap.enabled","false");
		}

		if(system.getUseStreamOutput() == 1){
			conf.set("mycat.stream.output.result","true");
		}else{
			conf.set("mycat.stream.output.result","false");
		}


		if(system.getMemoryPageSize() != null){
			conf.set("mycat.buffer.pageSize",system.getMemoryPageSize());
		}else{
			conf.set("mycat.buffer.pageSize","32k");
		}


		if(system.getSpillsFileBufferSize() != null){
			conf.set("mycat.merge.file.buffer",system.getSpillsFileBufferSize());
		}else{
			conf.set("mycat.merge.file.buffer","32k");
		}

		conf.set("mycat.pointer.array.len","1k")
			.set("mycat.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize));

		LOGGER.info("mycat.memory.offHeap.size: " +
				JavaUtils.bytesToString2(resultSetBufferSize));

		resultMergeMemoryManager =
				new ResultMergeMemoryManager(conf,numCores,maxOnHeapMemory);


		serializerManager = new SerializerManager();

		blockManager = new DataNodeDiskManager(conf,true,serializerManager);

	}


	@VisibleForTesting
	public MyCatMemory() throws NoSuchFieldException, IllegalAccessException {
		this.system = null;
		this.systemReserveBufferSize = 0;
		this.memoryPageSize = 0;
		this.spillsFileBufferSize = 0;
		conf = new MycatPropertyConf();
		numCores = Runtime.getRuntime().availableProcessors();

		long maxOnHeapMemory =  (Platform.getMaxHeapMemory());
		assert maxOnHeapMemory > 0;

		resultSetBufferSize = (long)((Platform.getMaxDirectMemory())*DIRECT_SAFETY_FRACTION);

		assert resultSetBufferSize > 0;
		/**
		 * mycat.memory.offHeap.enabled
		 * mycat.buffer.pageSize
		 * mycat.memory.offHeap.size
		 * mycat.testing.memory
		 * mycat.merge.file.buffer
		 * mycat.direct.output.result
		 * mycat.local.dir
		 */
		conf.set("mycat.memory.offHeap.enabled","true")
				.set("mycat.pointer.array.len","8K")
				.set("mycat.buffer.pageSize","1m")
				.set("mycat.memory.offHeap.size", JavaUtils.bytesToString2(resultSetBufferSize))
				.set("mycat.stream.output.result","false");

		LOGGER.info("mycat.memory.offHeap.size: " + JavaUtils.bytesToString2(resultSetBufferSize));

		resultMergeMemoryManager =
				new ResultMergeMemoryManager(conf,numCores,maxOnHeapMemory);

		serializerManager = new SerializerManager();

		blockManager = new DataNodeDiskManager(conf,true,serializerManager);

	}

		public MycatPropertyConf getConf() {
		return conf;
	}

	public long getResultSetBufferSize() {
		return resultSetBufferSize;
	}

	public MemoryManager getResultMergeMemoryManager() {
		return resultMergeMemoryManager;
	}

	public SerializerManager getSerializerManager() {
		return serializerManager;
	}

	public DataNodeDiskManager getBlockManager() {
		return blockManager;
	}

}
