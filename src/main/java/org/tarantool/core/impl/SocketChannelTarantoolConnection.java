package org.tarantool.core.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.tarantool.core.exception.CommunicationException;

/**
 * Connection to tarantool using {@link SocketChannel}
 */
public class SocketChannelTarantoolConnection extends TarantoolConnectionImpl {
	static SocketChannel channel(String host, int port) {
		try {
			return SocketChannel.open(new InetSocketAddress(host, port));
		} catch (IOException e) {
			throw new CommunicationException("Can't connecto to " + host + ":" + port, e);
		}
	}

	/**
	 * Creates connection using specified host and port
	 * 
	 * @param host
	 * @param port
	 */
	public SocketChannelTarantoolConnection(String host, int port) {
		super(new ByteChannelTransport(channel(host, port)));
	}

	/**
	 * Creates connection using default host and port
	 */
	public SocketChannelTarantoolConnection() {
		this("localhost", 33013);
	}

}
