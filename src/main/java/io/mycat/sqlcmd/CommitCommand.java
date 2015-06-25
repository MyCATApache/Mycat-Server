package io.mycat.sqlcmd;

import io.mycat.backend.BackendConnection;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.session.NonBlockingSession;

public class CommitCommand implements SQLCtrlCommand {

	@Override
	public void sendCommand(NonBlockingSession session, BackendConnection con) {
		con.commit();
	}

	@Override
	public void errorResponse(NonBlockingSession session, byte[] err,
			int total, int failed) {
		ErrorPacket errPkg = new ErrorPacket();
		errPkg.read(err);
		String errInfo = "total " + total + " failed " + failed + " detail:"
				+ new String(errPkg.message);
		session.getSource().setTxInterrupt(errInfo);
		errPkg.write(session.getSource());
	}

	@Override
	public void okResponse(NonBlockingSession session, byte[] ok) {
		session.getSource().write(ok);
	}

	@Override
	public boolean releaseConOnErr() {
		// need rollback when err
		return false;
	}

	@Override
	public boolean relaseConOnOK() {
		return true;
	}

	@Override
	public boolean isAutoClearSessionCons() {
		// need rollback when err
		return false;
	}

}
