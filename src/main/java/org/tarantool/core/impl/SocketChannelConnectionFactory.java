package org.tarantool.core.impl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.tarantool.core.ClientReturnPoint;
import org.tarantool.core.Returnable;
import org.tarantool.core.SingleQueryClientFactory;
import org.tarantool.core.TarantoolClient;

public class SocketChannelConnectionFactory implements SingleQueryClientFactory, ClientReturnPoint {
	String host = "localhost";
	int port = 33013;
	int minPoolSize = 10;
	int maxPoolSize = 100;
	BlockingQueue<TarantoolClient> pool;
	Semaphore connections;

	public SocketChannelConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
		super();
		this.host = host;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
		pool = new ArrayBlockingQueue<TarantoolClient>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public SocketChannelConnectionFactory(int minPoolSize, int maxPoolSize) {
		this("localhost", 33013, minPoolSize, maxPoolSize);

	}

	public SocketChannelConnectionFactory() {
		this("localhost", 33013, 10, 100);

	}

	public void afterPropertiesSet() {
		pool = minPoolSize < 1 ? null : new ArrayBlockingQueue<TarantoolClient>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public TarantoolClient newUnpooledConnection() {
		return new SocketChannelTarantoolClient(host, port);

	}

	public TarantoolClient getConnection() {
		try {
			connections.acquire();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		TarantoolClient connection = pool == null ? null : pool.poll();
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
	public TarantoolClient getSingleQueryConnection() {
		TarantoolClient connection = getConnection();
		if (connection instanceof Returnable) {
			((Returnable) connection).returnTo(this);
			return connection;
		} else {
			throw new IllegalArgumentException(connection.getClass()
					+ "  not able to works in single query mode, it should implements Returnable to use this feature");
		}
	}

	public void free() {
		TarantoolClient con = null;
		while ((con = pool.poll()) != null) {
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
	public void returnConnection(TarantoolClient connection) {
		try {
			if (!pool.offer(connection)) {
				connection.close();
			}
		} finally {
			connections.release();
		}
	}

}
