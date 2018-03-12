package io.mycat.manager.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.model.SystemConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.StringUtil;

/**
 * show Sysconfig param detail info
 * 
 * @author rainbow
 * 
 */
public class ShowSysParam {
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("PARAM_NAME", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PARAM_VALUE", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("PARAM_DESCR", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		
		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

        // write rows
        byte packetId = eof.packetId;
        
        SystemConfig sysConfig = MycatServer.getInstance().getConfig().getSystem();
        
        List<String> paramValues = new ArrayList<String>();
        paramValues.add(sysConfig.getProcessors() + "");
        paramValues.add(sysConfig.getBufferPoolChunkSize() + "B");
        paramValues.add(sysConfig.getBufferPoolPageSize() + "B");
        paramValues.add(sysConfig.getProcessorBufferLocalPercent() + "");
        paramValues.add(sysConfig.getProcessorExecutor() + "");
        paramValues.add(sysConfig.getSequnceHandlerType() == 1 ? "数据库方式" : "本地文件方式");
        paramValues.add(sysConfig.getPacketHeaderSize() + "B");
        paramValues.add(sysConfig.getMaxPacketSize()/1024/1024 + "M");
        paramValues.add(sysConfig.getIdleTimeout()/1000/60 + "分钟");
        paramValues.add(sysConfig.getCharset() + "");
        paramValues.add(ISOLATIONS[sysConfig.getTxIsolation()]);
        paramValues.add(sysConfig.getSqlExecuteTimeout() + "秒");
        paramValues.add(sysConfig.getProcessorCheckPeriod()/1000 + "秒");
        paramValues.add(sysConfig.getDataNodeIdleCheckPeriod()/1000 + "秒");
        paramValues.add(sysConfig.getDataNodeHeartbeatPeriod()/1000 + "秒");
        paramValues.add(sysConfig.getBindIp() + "");
        paramValues.add(sysConfig.getServerPort()+ "");
        paramValues.add(sysConfig.getManagerPort() + "");

		for(int i = 0; i < PARAMNAMES.length ; i++){
	        RowDataPacket row =  new RowDataPacket(FIELD_COUNT);
	        row.add(StringUtil.encode(PARAMNAMES[i], c.getCharset()));
	        row.add(StringUtil.encode(paramValues.get(i), c.getCharset()));
	        row.add(StringUtil.encode(PARAM_DESCRIPTION[i], c.getCharset()));
	        row.packetId = ++packetId;
	        buffer = row.write(buffer, c,true);
		}
		
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}
	
	private static final String[] PARAMNAMES = {
		"processors",
		"processorBufferChunk",
		"processorBufferPool",
		"processorBufferLocalPercent",
		"processorExecutor",
		"sequnceHandlerType",
		"Mysql_packetHeaderSize",
		"Mysql_maxPacketSize",
		"Mysql_idleTimeout",
		"Mysql_charset",
		"Mysql_txIsolation",
		"Mysql_sqlExecuteTimeout",
		"Mycat_processorCheckPeriod",
		"Mycat_dataNodeIdleCheckPeriod",
		"Mycat_dataNodeHeartbeatPeriod",
		"Mycat_bindIp",
		"Mycat_serverPort",
		"Mycat_managerPort"};
	
	private static final String[] PARAM_DESCRIPTION = {
		"主要用于指定系统可用的线程数，默认值为Runtime.getRuntime().availableProcessors()方法返回的值。主要影响processorBufferPool、processorBufferLocalPercent、processorExecutor属性。NIOProcessor的个数也是由这个属性定义的，所以调优的时候可以适当的调高这个属性。",
		"指定每次分配Socket Direct Buffer的大小，默认是4096个字节。这个属性也影响buffer pool的长度。",
		"指定bufferPool计算 比例值。由于每次执行NIO读、写操作都需要使用到buffer，系统初始化的时候会建立一定长度的buffer池来加快读、写的效率，减少建立buffer的时间",
		"就是用来控制分配这个pool的大小用的，但其也并不是一个准确的值，也是一个比例值。这个属性默认值为100。线程缓存百分比 = bufferLocalPercent / processors属性。",
		"主要用于指定NIOProcessor上共享的businessExecutor固定线程池大小。mycat在需要处理一些异步逻辑的时候会把任务提交到这个线程池中。新版本中这个连接池的使用频率不是很大了，可以设置一个较小的值。",
		"指定使用Mycat全局序列的类型。",
		"指定Mysql协议中的报文头长度。默认4",
		"指定Mysql协议可以携带的数据最大长度。默认16M",
		"指定连接的空闲超时时间。某连接在发起空闲检查下，发现距离上次使用超过了空闲时间，那么这个连接会被回收，就是被直接的关闭掉。默认30分钟",
		"连接的初始化字符集。默认为utf8",
		"前端连接的初始化事务隔离级别，只在初始化的时候使用，后续会根据客户端传递过来的属性对后端数据库连接进行同步。默认为REPEATED_READ",
		"SQL执行超时的时间，Mycat会检查连接上最后一次执行SQL的时间，若超过这个时间则会直接关闭这连接。默认时间为300秒",
		"清理NIOProcessor上前后端空闲、超时和关闭连接的间隔时间。默认是1秒",
		"对后端连接进行空闲、超时检查的时间间隔，默认是300秒",
		"对后端所有读、写库发起心跳的间隔时间，默认是10秒",
		"mycat服务监听的IP地址，默认值为0.0.0.0",
		"mycat的使用端口，默认值为8066",
		"mycat的管理端口，默认值为9066"};
	
    public static final String[] ISOLATIONS = {"", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATED_READ", "SERIALIZABLE"};
}
