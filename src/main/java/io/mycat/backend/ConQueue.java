package io.mycat.backend;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 连接队列
 */
public class ConQueue {
	// 自动提交连接队列
	private final ConcurrentLinkedQueue<BackendConnection> autoCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	// 手动提交连接队列
	private final ConcurrentLinkedQueue<BackendConnection> manCommitCons = new ConcurrentLinkedQueue<BackendConnection>();
	// 执行次数
	private long executeCount;

	/**
	 * 获取空闲连接
	 * @param autoCommit
	 * @return
	 */
	public BackendConnection takeIdleCon(boolean autoCommit) {
		ConcurrentLinkedQueue<BackendConnection> f1 = autoCommitCons;
		ConcurrentLinkedQueue<BackendConnection> f2 = manCommitCons;

		if (!autoCommit) {
			f1 = manCommitCons;
			f2 = autoCommitCons;
		}
		BackendConnection con = f1.poll();
		if (con == null || con.isClosedOrQuit()) {
			con = f2.poll();
		}
		if (con == null || con.isClosedOrQuit()) {
			return null;
		} else {
			return con;
		}
	}

	/**
	 * 获取执行次数
	 * @return
	 */
	public long getExecuteCount() {
		return executeCount;
	}

	/**
	 * 增加执行次数
	 */
	public void incExecuteCount() {
		this.executeCount++;
	}

	/**
	 * 移除连接
	 * @param con
	 * @return
	 */
	public boolean removeCon(BackendConnection con) {
		boolean removed = autoCommitCons.remove(con);
		if (!removed) {
			return manCommitCons.remove(con);
		}
		return removed;
	}

	/**
	 * 是否有这个连接
	 * @param con
	 * @return
	 */
	public boolean isSameCon(BackendConnection con) {
		if (autoCommitCons.contains(con)) {
			return true;
		} else if (manCommitCons.contains(con)) {
			return true;
		}
		return false;
	}

	public ConcurrentLinkedQueue<BackendConnection> getAutoCommitCons() {
		return autoCommitCons;
	}

	public ConcurrentLinkedQueue<BackendConnection> getManCommitCons() {
		return manCommitCons;
	}

	/**
	 * 获取将要关闭的空闲连接
	 * @param count
	 * @return
	 */
	public ArrayList<BackendConnection> getIdleConsToClose(int count) {
		ArrayList<BackendConnection> readyCloseCons = new ArrayList<BackendConnection>(count);
		while (!manCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = manCommitCons.poll();
			if (theCon != null && !theCon.isBorrowed()) {
				readyCloseCons.add(theCon);
			}
		}
		while (!autoCommitCons.isEmpty() && readyCloseCons.size() < count) {
			BackendConnection theCon = autoCommitCons.poll();
			if (theCon != null && !theCon.isBorrowed()) {
				readyCloseCons.add(theCon);
			}
		}
		return readyCloseCons;
	}
}
