package org.tarantool.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class ConnectionFactory {
	String host = "localhost";
	int port = 33013;
	int minPoolSize = 1;
	int maxPoolSize = 10;
	BlockingQueue<Connection> basePool;
	Semaphore connections;

	public ConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
		super();
		this.host = host;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
		basePool = new ArrayBlockingQueue<Connection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public ConnectionFactory(int minPoolSize, int maxPoolSize) {
		this("localhost", 33013, minPoolSize, maxPoolSize);

	}

	public ConnectionFactory() {
		super();
		basePool = new ArrayBlockingQueue<Connection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public void afterPropertiesSet() {
		basePool = new ArrayBlockingQueue<Connection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	public Connection newUnpooledConnection() {
		return new Connection(host, port);
	}

	public Connection getConnection() {
		try {
			connections.acquire();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
		Connection connection = basePool.poll();
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

	public Connection getSingleQueryConnection() {
		Connection connection = getConnection();
		connection.autoReturn(this);
		return connection;

	}

	public void returnConnection(Connection con) {
		try {
			if (con.channel.isOpen()) {
				if (!basePool.offer(con)) {
					con.close();
				}
			}
		} finally {
			connections.release();
		}
	}

	public void free() {
		Connection con = null;
		while ((con = basePool.poll()) != null) {
			con.close();
		}
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}
}
