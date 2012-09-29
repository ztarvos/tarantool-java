package org.tarantool.it;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.tarantool.snapshot.ReplicationClient;
import org.tarantool.snapshot.XLogReader.XLogEntry;

public class TestReplication {
	// @Test
	public void testReplication() throws IOException {
		ReplicationClient client = new ReplicationClient(SocketChannel.open(new InetSocketAddress("localhost", 33316)), 1L);
		for (int i = 0; i < 10; i++) {
			XLogEntry r = client.nextEntry();
		}
		client.close();
	}

}
