package io.mycat.util.rehasher;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.PartitionByMod;
import io.mycat.route.function.PartitionByMurmurHash;
import io.mycat.util.CollectionUtil;
import io.mycat.util.exception.RehashException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 本工具依赖druid，Mycat已经包含druid，druid配置请查阅相关文档。相关参数请看RehashCmdArgs
 * @author wujingrun
 *
 */
public class RehashLauncher {
	private final class RehashRunner implements Runnable {
        private final File   output;
        private final String table;
        
        private RehashRunner(File output, String table) {
            this.output = output;
            this.table = table;
        }
        
        public void run(){
        	int pageSize=500;
        	int page=0;
        	List<Map<String, Object>> list=null;
        	
        	int total=0;
        	int rehashed=0;
        	String hostWithDatabase=args.getHostWithDatabase();
        	
        	PrintStream ps=null;
        	try {
        		ps=new PrintStream(output);
        		list = JdbcUtils.executeQuery(dataSource, "select "
                    + args.getShardingField() + " from " + table + " limit ?,?", page++ * pageSize,
                    pageSize);
                while (!CollectionUtil.isEmpty(list)) {
        			for(int i=0,l=list.size();i<l;i++){
        				Map<String, Object> sf=list.get(i);
        				Integer hash=alg.calculate(sf.get(args.getShardingField()).toString());
        				String host=rehashHosts[hash];
        				total++;
        				if(host.equals(hostWithDatabase)){
        					rehashed++;
        				}
        				ps.println(sf+"=>"+host);
        			}
        			list = JdbcUtils.executeQuery(dataSource, "select "
                        + args.getShardingField() + " from " + table + " limit ?,?", page++ * pageSize,
                        pageSize);
        		}
        		ps.println("rehashed ratio:"+(((double)rehashed)/total));
        	} catch (Exception e) {
        		throw new RehashException(e);
        	}finally{
        		if(ps!=null){
        			ps.close();
        		}
        		latch.countDown();
        	}
        }
    }

    private RehashCmdArgs args;
	private DruidDataSource dataSource;
	private String[] rehashHosts;
	private AbstractPartitionAlgorithm alg;
	private ExecutorService executor;
	private CountDownLatch latch;
    private static final Logger        LOGGER = LoggerFactory.getLogger(RehashLauncher.class);
	
	private RehashLauncher(String[] args) throws IOException{
		this.args=new RehashCmdArgs(args);
		initDataSource();
		this.rehashHosts=this.args.getRehashHosts();
		initHashAlg();
		executor=Executors.newCachedThreadPool();
	}
	
	private void initHashAlg() throws IOException{
	    if (HashType.MURMUR.equals(args.getHashType())) {
	        alg=new PartitionByMurmurHash();
            PartitionByMurmurHash murmur=(PartitionByMurmurHash)alg;
            murmur.setCount(rehashHosts.length);
            murmur.setSeed(args.getMurmurHashSeed());
            murmur.setVirtualBucketTimes(args.getMurmurHashVirtualBucketTimes());
            murmur.setWeightMapFile(args.getMurmurWeightMapFile());
            murmur.init();
	    } else if (HashType.MOD.equals(args.getHashType())) {
	        alg=new PartitionByMod();
            PartitionByMod mod=(PartitionByMod)alg;
            mod.setCount(rehashHosts.length);
            mod.init();
        }
	}
	
	private void initDataSource(){
		dataSource=new DruidDataSource();
		dataSource.setAsyncCloseConnectionEnable(true);
		dataSource.setBreakAfterAcquireFailure(true);
		dataSource.setDefaultAutoCommit(true);
		dataSource.setDefaultReadOnly(true);
		dataSource.setDriverClassName(args.getJdbcDriver());
		dataSource.setEnable(true);
		dataSource.setPassword(args.getPassword());
		dataSource.setTestOnBorrow(true);
		dataSource.setTestOnReturn(true);
		dataSource.setTestWhileIdle(true);
		dataSource.setUrl(args.getJdbcUrl());
		dataSource.setUsername(args.getUser());
	}
	
	private RehashLauncher execute() throws IOException{
		final String[] tables=args.getTables();
		final File outputDir=new File(args.getRehashNodeDir());
		if(!outputDir.exists()){
			outputDir.mkdirs();
		}else if(outputDir.isFile()){
			throw new IllegalArgumentException("rehashNodeDir must be a directory");
		}else if(outputDir.canWrite()){
			throw new IllegalArgumentException("rehashNodeDir must be writable");
		}
		latch=new CountDownLatch(tables.length);
		for(int i=0,l=tables.length;i<l;i++){
			final int tableIdx=i;
			final String table=tables[tableIdx];
			final File output=new File(outputDir,table);
			if(output.exists()){
				output.delete();
			}
			output.createNewFile();
			executor.execute(new RehashRunner(output, table));
		}
		return this;
	}
	
	private void shutdown(){
		while(true){
			try {
				latch.await();
				break;
			} catch (InterruptedException e) {
			    LOGGER.error("RehashLauncherError", e);
			}
		}
		executor.shutdown();
		if(executor.isTerminated()){
			dataSource.close();
		}
	}
	
	private static void execute(String[] args) throws IOException{
		RehashLauncher launcher=null;
		try{
			launcher=new RehashLauncher(args).execute();
		} catch (IOException e) {
            LOGGER.error("RehashLauncherError", e);
            throw e;
        } finally{
			if(launcher!=null){
				launcher.shutdown();
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		execute(args);
	}
}
