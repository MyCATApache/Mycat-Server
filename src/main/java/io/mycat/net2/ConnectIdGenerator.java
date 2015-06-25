package io.mycat.net2;

/**
 * 连接ID生成器
 * 
 * @author mycat
 */
public class ConnectIdGenerator {

	private static final long MAX_VALUE = Long.MAX_VALUE;
	private static ConnectIdGenerator instance=new ConnectIdGenerator();
    public static ConnectIdGenerator getINSTNCE()
    {
    	return instance;
    }
	private long connectId = 0L;
	private final Object lock = new Object();

	public long getId() {
		synchronized (lock) {
			if (connectId >= MAX_VALUE) {
				connectId = 0L;
			}
			return ++connectId;
		}
	}
}