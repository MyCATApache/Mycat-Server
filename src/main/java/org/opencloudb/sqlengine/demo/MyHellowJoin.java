package org.opencloudb.sqlengine.demo;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.sqlengine.EngineCtx;
import org.opencloudb.sqlengine.SQLJobHandler;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.ResultSetUtil;

public class MyHellowJoin {

	public void processSQL(String sql, EngineCtx ctx) {

		DirectDBJoinHandler joinHandler = new DirectDBJoinHandler(ctx);
		String[] dataNodes = { "dn1", "dn2", "dn3" };
		ctx.executeNativeSQLSequnceJob(dataNodes, sql, joinHandler);
	}
}

class DirectDBJoinHandler implements SQLJobHandler {
	private List<byte[]> fields;
	private byte[] header;
	private final EngineCtx ctx;

	public DirectDBJoinHandler(EngineCtx ctx) {
		super();
		this.ctx = ctx;
	}

	private Map<String, byte[]> rows = new ConcurrentHashMap<String, byte[]>();
	private ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<String>();

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		if (this.fields == null) {
			this.fields = fields;
			this.header = header;
		}

	}

	private void createQryJob(int batchSize) {
		int count = 0;
		Map<String, byte[]> batchRows = new ConcurrentHashMap<String, byte[]>();
		String theId = null;
		StringBuilder sb = new StringBuilder().append('(');
		while ((theId = ids.poll()) != null) {
			batchRows.put(theId, rows.remove(theId));
			sb.append(theId).append(',');
			if (count++ > batchSize) {
				break;
			}
		}
		if (count == 0) {
			return;
		}
		sb.deleteCharAt(sb.length() - 1).append(')');
		String querySQL = "select b.id, b.title  from hotnews b where id in "
				+ sb;
		ctx.executeNativeSQLParallJob(new String[] { "dn1", "dn2", "dn3" },
				querySQL, new MyRowOutPutDataHandler(fields, ctx, batchRows));
	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {

		String id = ResultSetUtil.getColumnValAsString(rowData, fields, 0);
		// 放入结果集
		rows.put(id, rowData);
		ids.offer(id);

		int batchSize = 999;
		// 满1000条，发送一个查询请求
		if (ids.size() > batchSize) {
			createQryJob(batchSize);
		}

		return false;
	}

	@Override
	public void finished(String dataNode, boolean failed) {
		if (!failed) {
			createQryJob(Integer.MAX_VALUE);
		}
		// no more jobs
		ctx.endJobInput();
	}

}

class MyRowOutPutDataHandler implements SQLJobHandler {
	private final List<byte[]> afields;
	private List<byte[]> bfields;
	private final EngineCtx ctx;
	private final Map<String, byte[]> arows;
	private final ReentrantLock writeLock = new ReentrantLock();

	public MyRowOutPutDataHandler(List<byte[]> afields, EngineCtx ctx,
			Map<String, byte[]> arows) {
		super();
		this.afields = afields;
		this.ctx = ctx;
		this.arows = arows;
	}

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		try {
			writeLock.lock();
			if (bfields != null) {
				return;
			}
			bfields = fields;
			// write new header
			ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
			headerPkg.fieldCount = afields.size() + 1;
			headerPkg.packetId = ctx.incPackageId();
			ctx.LOGGER.debug("packge id " + headerPkg.packetId);
			ServerConnection sc = ctx.getSession().getSource();
			ByteBuffer buf = headerPkg.write(sc.allocate(), sc, true);
			// wirte a fields
			for (byte[] field : afields) {
				field[3] = ctx.incPackageId();
				buf = sc.writeToBuffer(field, buf);
			}
			// write b field
			byte[] bfield = fields.get(1);
			bfield[3] = ctx.incPackageId();
			buf = sc.writeToBuffer(bfield, buf);

			// write field eof
			EOFPacket eofPckg = new EOFPacket();
			eofPckg.packetId = ctx.incPackageId();
			buf = eofPckg.write(buf, sc, true);
			sc.write(buf);
			// EngineCtx.LOGGER.info("header outputed");
		} finally {
			writeLock.unlock();
		}

	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		RowDataPacket rowDataPkg = ResultSetUtil.parseRowData(rowData, bfields);
		// 获取Id字段，
		String id = ByteUtil.getString(rowDataPkg.fieldValues.get(0));
		byte[] bname = rowDataPkg.fieldValues.get(1);
		// 查找ID对应的A表的记录
		byte[] arow = arows.remove(id);
		rowDataPkg = ResultSetUtil.parseRowData(arow, afields);
		// 设置b.name 字段
		rowDataPkg.add(bname);

		ServerConnection sc = ctx.getSession().getSource();
		try {
			writeLock.lock();
			rowDataPkg.packetId = ctx.incPackageId();
			// 输出完整的 记录到客户端
			ByteBuffer buf = rowDataPkg.write(sc.allocate(), sc, true);
			sc.write(buf);
		} finally {
			writeLock.unlock();
		}
		// EngineCtx.LOGGER.info("out put row ");
		return false;
	}

	@Override
	public void finished(String dataNode, boolean failed) {

	}
}
