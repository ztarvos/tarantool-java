package org.tarantool.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.tarantool.core.exception.CommunicationException;

public class SocketChannelConnectionFactory implements SingleQueryConnectionFactory, ConnectionReturnPoint {
	String host = "localhost";
	int port = 33013;
	int minPoolSize = 0;
	int maxPoolSize = 100;
	BlockingQueue<Connection> pool;
	Semaphore connections;

	public SocketChannelConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
		super();
		this.host = host;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
		pool = minPoolSize < 1 ? null : new ArrayBlockingQueue<Connection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public SocketChannelConnectionFactory(int minPoolSize, int maxPoolSize) {
		this("localhost", 33013, minPoolSize, maxPoolSize);

	}

	public SocketChannelConnectionFactory() {
		this("localhost", 33013, 0, 100);

	}

	public void afterPropertiesSet() {
		pool = minPoolSize < 1 ? null : new ArrayBlockingQueue<Connection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public Connection newUnpooledConnection() {
		try {
			return new ConnectionImpl(new ByteChannelTransport(openChannel()));
		} catch (IOException e) {
			throw new CommunicationException("Can't create connection", e);
		}
	}

	protected SocketChannel openChannel() throws IOException {
		return SocketChannel.open(new InetSocketAddress(host, port));
	}

	public Connection getConnection() {
		try {
			connections.acquire();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		Connection connection = pool == null ? null : pool.poll();
		if (connection == null) {
			connection = newUnpooledConnection();
		} else {
			try {
				connection.ping();
			} catch (Exception e) {
				connection = newUnpooledConnection();
			}
		}

		return connection;
	}

	@Override
	public Connection getSingleQueryConnection() {
		Connection connection = getConnection();
		if (connection instanceof Returnable) {
			((Returnable) connection).returnTo(this);
			return connection;
		} else {
			throw new IllegalArgumentException(connection.getClass()
					+ "  not able to works in single query mode, it should implements Returnable to use this feature");
		}
	}

	public void free() {
		Connection con = null;
		while (pool != null && (con = pool.poll()) != null) {
			con.close();
		}
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public void returnConnection(Connection connection) {
		try {
			if (pool == null || !pool.offer(connection)) {
				connection.close();
			}
		} finally {
			connections.release();
		}
	}

}
