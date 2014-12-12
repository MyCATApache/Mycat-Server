package org.opencloudb.sqlengine;

import org.opencloudb.config.ErrorCode;
import org.opencloudb.server.NonBlockingSession;

public class MutilNodeCoordListener {

	public void failed(String errMsg, int totalNodes, int failedNodes,
			NonBlockingSession session) {
		session.getSource()
				.writeErrMessage(
						ErrorCode.ERR_MULTI_NODE_FAILED,
						"total " + totalNodes + ",failed " + failedNodes + " "
								+ errMsg);

	}

	public void finished(NonBlockingSession session) {

	}
}
