package org.tarantool.core.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.tarantool.core.exception.CommunicationException;

public class SocketChannelTarantoolClient extends TarantoolClientImpl {
	static SocketChannel channel(String host, int port) {
		try {
			return SocketChannel.open(new InetSocketAddress(host, port));
		} catch (IOException e) {
			throw new CommunicationException("Can't connecto to " + host + ":" + port, e);
		}
	}

	public SocketChannelTarantoolClient(String host, int port) {
		super(new ByteChannelTransport(channel(host, port)));
	}

	public SocketChannelTarantoolClient() {
		this("localhost", 33013);
	}

}
