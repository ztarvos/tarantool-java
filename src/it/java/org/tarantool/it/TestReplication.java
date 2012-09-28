package org.tarantool.it;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.tarantool.core.Tuple;
import org.tarantool.snapshot.ReplicationClient;

public class TestReplication {
//	@Test
	public void testReplication() throws IOException {
		ReplicationClient client = new ReplicationClient(SocketChannel.open(new InetSocketAddress("localhost", 33316)), 1L);
		Tuple t = client.readTuple();
		client.close();
	}

}
