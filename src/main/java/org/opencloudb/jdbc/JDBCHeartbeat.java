package org.opencloudb.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.locks.ReentrantLock;
import org.opencloudb.heartbeat.DBHeartbeat;

public class JDBCHeartbeat extends DBHeartbeat{
	private final ReentrantLock lock;
	private final JDBCDatasource source;
    private final boolean heartbeatnull;
	public JDBCHeartbeat(JDBCDatasource source)
	{
		this.source = source;
		lock = new ReentrantLock(false);
		this.status = INIT_STATUS;
		this.heartbeatSQL = source.getHostConfig().getHearbeatSQL().trim();
		this.heartbeatnull= heartbeatSQL.length()==0;
	}

	@Override
	public void start()
	{
		if (this.heartbeatnull){
			stop();
			return;
		}
		lock.lock();
		try
		{
			isStop.compareAndSet(true, false);
			this.status = DBHeartbeat.OK_STATUS;
		} finally
		{
			lock.unlock();
		}
	}

	@Override
	public void stop()
	{
		lock.lock();
		try
		{
			if (isStop.compareAndSet(false, true))
			{
				isChecking.set(false);
			}
		} finally
		{
			lock.unlock();
		}
	}

	@Override
	public String getLastActiveTime()
	{
		return null;
	}

	@Override
	public long getTimeout()
	{
		return 0;
	}

	@Override
	public void heartbeat()
	{
		if (isStop.get())
			return;

		lock.lock();
		try
		{
			isChecking.set(true);

			try (Connection c = source.getConnection())
			{
				try (Statement s = c.createStatement())
				{
					s.execute(heartbeatSQL);
				}
			}
			status = OK_STATUS;

		} catch (Exception ex)
		{
			status = ERROR_STATUS;
		} finally
		{
			lock.unlock();
			this.isChecking.set(false);
		}
	}
}
