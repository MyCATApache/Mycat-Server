package org.opencloudb.performance;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class UserTableInsertJob implements Runnable {
	private long finsihed;
	private final long batchSize;
	private final AtomicLong finshiedCount;
	private final AtomicLong failedCount;

	private final SimpleConPool conPool;
	private final long totalRecords;
	private LinkedList<StringItem> sqlTemplateItems;
	private final boolean autocommit;

	public UserTableInsertJob(SimpleConPool conPool, long totalRecords,
			long batchSize, AtomicLong finshiedCount, AtomicLong failedCount,
			LinkedList<StringItem> sqlTemplateItems, boolean autoCommit) {
		super();
		this.conPool = conPool;
		this.totalRecords = totalRecords;
		this.batchSize = batchSize;
		this.finshiedCount = finshiedCount;
		this.failedCount = failedCount;
		this.sqlTemplateItems = sqlTemplateItems;
		this.autocommit = autoCommit;
	}

	private long insert(Connection con, List<String> list) throws SQLException {
		Statement stms = con.createStatement();
		for (String sql : list) {
			stms.addBatch(sql);
		}
		stms.executeBatch();
		if (this.autocommit == false) {
			con.commit();
		}
		// stms.clearBatch();
		stms.close();
		return list.size();
	}

	private List<String> getNextBatch() {
		if (finsihed >= totalRecords) {
			return Collections.emptyList();
		}
		int curBatchSize = (int) ((finsihed + batchSize) < totalRecords ? batchSize
				: (totalRecords - finsihed));
		ArrayList<String> list = new ArrayList<String>(curBatchSize);
		for (long i = 0; i < curBatchSize; i++) {
			list.add(RandomDataValueUtil.evalRandValueString(sqlTemplateItems));
		}
		// System.out.println("finsihed :" + finsihed + "-" + end);
		finsihed += list.size();
		return list;
	}

	@Override
	public void run() {
		Connection con = null;
		try {

			List<String> batch = getNextBatch();
			while (!batch.isEmpty()) {
				try {
					if (con == null || con.isClosed()) {
						con = conPool.getConnection();
						if (con.getAutoCommit() != autocommit) {
							con.setAutoCommit(autocommit);
						}
					}

					insert(con, batch);
					finshiedCount.addAndGet(batch.size());
				} catch (Exception e) {
					System.out.println("caught err in  thread :"
							+ Thread.currentThread().getId() + " " + e);
					try {
						con.rollback();
					} catch (SQLException e1) {
						System.out.println("caught err in thread :"
								+ Thread.currentThread().getId()
								+ " rollback err " + e1);
					}
					failedCount.addAndGet(batch.size());
				}
				batch = getNextBatch();
			}
		} finally {
			if (con != null) {
				this.conPool.returnCon(con);
			}
		}

	}
}