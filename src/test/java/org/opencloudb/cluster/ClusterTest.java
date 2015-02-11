package org.opencloudb.cluster;

import org.junit.Assert;
import org.junit.Test;
import org.opencloudb.cluster.heartbeat.HeartServer;

public class ClusterTest {

	@Test
	public void test() {
		boolean flag = HeartServer.beat("jdbc:mysql://localhost:9092/mycat", "mycat","mycat2");
		Assert.assertEquals(flag,true);
	}
}
