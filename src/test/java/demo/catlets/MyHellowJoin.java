package demo.catlets;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.mycat.cache.LayerCachePool;
import io.mycat.catlets.Catlet;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.sqlengine.AllJobFinishedListener;
import io.mycat.sqlengine.EngineCtx;
import io.mycat.sqlengine.SQLJobHandler;
import io.mycat.util.ByteUtil;
import io.mycat.util.ResultSetUtil;

public class MyHellowJoin implements Catlet {

	public void processSQL(String sql, EngineCtx ctx) {

		DirectDBJoinHandler joinHandler = new DirectDBJoinHandler(ctx);
		String[] dataNodes = { "dn1", "dn2", "dn3" };
		ctx.executeNativeSQLSequnceJob(dataNodes, sql, joinHandler);
		ctx.setAllJobFinishedListener(new AllJobFinishedListener() {

			@Override
			public void onAllJobFinished(EngineCtx ctx) {
				ctx.writeEof();

			}
		});
	}

	@Override
	public void route(SystemConfig sysConfig, SchemaConfig schema, int sqlType,
			String realSQL, String charset, ServerConnection sc,
			LayerCachePool cachePool) {
		
		
	}
}

class DirectDBJoinHandler implements SQLJobHandler {
	private List<byte[]> fields;
	private final EngineCtx ctx;

	public DirectDBJoinHandler(EngineCtx ctx) {
		super();
		this.ctx = ctx;
	}

	private Map<String, byte[]> rows = new ConcurrentHashMap<String, byte[]>();
	private ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<String>();

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		this.fields = fields;

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
	public void finished(String dataNode, boolean failed, String errorMsg) {
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

	public MyRowOutPutDataHandler(List<byte[]> afields, EngineCtx ctx,
			Map<String, byte[]> arows) {
		super();
		this.afields = afields;
		this.ctx = ctx;
		this.arows = arows;
	}

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> bfields) {
		this.bfields=bfields;
		ctx.writeHeader(afields, bfields);
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

		ctx.writeRow(rowDataPkg);
		// EngineCtx.LOGGER.info("out put row ");
		return false;
	}

	@Override
	public void finished(String dataNode, boolean failed, String errorMsg) {

	}
}
