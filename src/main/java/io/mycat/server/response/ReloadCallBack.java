package io.mycat.server.response;

import com.google.common.util.concurrent.FutureCallback;
import io.mycat.server.ErrorCode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.config.loader.ReloadUtil;
import io.mycat.server.packet.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步执行回调类，用于回写数据给用户等。
 */
public class ReloadCallBack implements FutureCallback<Boolean> {
	private static final Logger LOGGER = LoggerFactory.getLogger(ReloadUtil.class);

	private MySQLFrontConnection mc;

	public ReloadCallBack(MySQLFrontConnection c) {
		this.mc = c;
	}

	@Override
	public void onSuccess(Boolean result) {
		if (result) {
			LOGGER.warn("send ok package to client " + String.valueOf(mc));
			OkPacket ok = new OkPacket();
			ok.packetId = 1;
			ok.affectedRows = 1;
			ok.serverStatus = 2;
			ok.message = "Reload config success".getBytes();
			ok.write(mc);
		} else {
			mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
		}
	}

	@Override
	public void onFailure(Throwable t) {
		mc.writeErrMessage(ErrorCode.ER_YES, "Reload config failure");
	}
}
