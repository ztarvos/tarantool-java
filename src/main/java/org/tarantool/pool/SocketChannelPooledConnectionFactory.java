package org.tarantool.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

/**
 * Simple pooled connection factory to tarantool server
 */
public class SocketChannelPooledConnectionFactory implements SingleQueryConnectionFactory, ConnectionReturnPoint {
	String host = "localhost";
	int port = 33013;
	int minPoolSize = 10;
	int maxPoolSize = 100;
	BlockingQueue<TarantoolConnection> pool;
	Semaphore connections;

	public SocketChannelPooledConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
		super();
		this.host = host;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
		pool = new ArrayBlockingQueue<TarantoolConnection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public SocketChannelPooledConnectionFactory(int minPoolSize, int maxPoolSize) {
		this("localhost", 33013, minPoolSize, maxPoolSize);

	}

	public SocketChannelPooledConnectionFactory() {
		this("localhost", 33013, 10, 100);

	}

	public void afterPropertiesSet() {
		pool = minPoolSize < 1 ? null : new ArrayBlockingQueue<TarantoolConnection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * @return unpooled connection that should be closed by close method call
	 */
	public TarantoolConnection newUnpooledConnection() {
		return new SocketChannelTarantoolConnection(host, port);

	}

	/**
	 * @return pooled connection that should be returned using returnConnection
	 *         method
	 */
	public TarantoolConnection getConnection() {
		try {
			connections.acquire();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		TarantoolConnection connection = pool == null ? null : pool.poll();
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

	/**
	 * @return pooled connection that will be returned to pool after first call,
	 *         should be used only once
	 */
	@Override
	public TarantoolConnection getSingleQueryConnection() {
		TarantoolConnection connection = getConnection();
		if (connection instanceof Returnable) {
			((Returnable) connection).returnTo(this);
			return connection;
		} else {
			throw new IllegalArgumentException(connection.getClass()
					+ "  not able to works in single query mode, it should implements Returnable to use this feature");
		}
	}

	/**
	 * Closes all opened connections
	 */
	public void free() {
		TarantoolConnection con = null;
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
	public void returnConnection(TarantoolConnection connection) {
		try {
			if (!pool.offer(connection)) {
				connection.close();
			}
		} finally {
			connections.release();
		}
	}

}
