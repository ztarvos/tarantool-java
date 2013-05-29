package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Simple pooled connection factory to tarantool server
 * 
 * @author dgreen
 * @version $Id: $
 */
public class SocketChannelPooledConnectionFactory implements SingleQueryConnectionFactory, ConnectionReturnPoint {
	String host = "localhost";
	int port = 33013;
	int minPoolSize = 10;
	int maxPoolSize = 100;
	BlockingQueue<TarantoolConnection> pool;
	Semaphore connections;

	/**
	 * <p>
	 * Constructor for SocketChannelPooledConnectionFactory.
	 * </p>
	 * 
	 * @param host
	 *            a {@link java.lang.String} object.
	 * @param port
	 *            a int.
	 * @param minPoolSize
	 *            a int.
	 * @param maxPoolSize
	 *            a int.
	 */
	public SocketChannelPooledConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
		super();
		this.host = host;
		this.port = port;
		this.minPoolSize = minPoolSize;
		this.maxPoolSize = maxPoolSize;
		pool = new ArrayBlockingQueue<TarantoolConnection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	/**
	 * <p>
	 * Constructor for SocketChannelPooledConnectionFactory.
	 * </p>
	 * 
	 * @param minPoolSize
	 *            a int.
	 * @param maxPoolSize
	 *            a int.
	 */
	public SocketChannelPooledConnectionFactory(int minPoolSize, int maxPoolSize) {
		this("localhost", 33013, minPoolSize, maxPoolSize);

	}

	/**
	 * <p>
	 * Constructor for SocketChannelPooledConnectionFactory.
	 * </p>
	 */
	public SocketChannelPooledConnectionFactory() {
		this("localhost", 33013, 10, 100);

	}

	/**
	 * <p>
	 * afterPropertiesSet.
	 * </p>
	 */
	public void afterPropertiesSet() {
		pool = minPoolSize < 1 ? null : new ArrayBlockingQueue<TarantoolConnection>(minPoolSize);
		connections = new Semaphore(maxPoolSize);
	}

	/**
	 * <p>
	 * Setter for the field <code>minPoolSize</code>.
	 * </p>
	 * 
	 * @param minPoolSize
	 *            a int.
	 */
	public void setMinPoolSize(int minPoolSize) {
		this.minPoolSize = minPoolSize;
	}

	/**
	 * <p>
	 * Setter for the field <code>maxPoolSize</code>.
	 * </p>
	 * 
	 * @param maxPoolSize
	 *            a int.
	 */
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * <p>
	 * newUnpooledConnection.
	 * </p>
	 * 
	 * @return unpooled connection that should be closed by close method call
	 */
	public TarantoolConnection newUnpooledConnection() {
		return new SocketChannelTarantoolConnection(host, port);

	}

	/**
	 * <p>
	 * getConnection.
	 * </p>
	 * 
	 * @return pooled connection that should be returned using returnConnection
	 *         method
	 */
	public TarantoolConnection getConnection() {

        TarantoolConnection connection = pool == null ? null : pool.poll();
		try {
			connections.acquire();

            if (connection == null) {
                connection = newUnpooledConnection();
            } else {
                try {
                    connection.ping();
                } catch (Exception e) {
                    connection = newUnpooledConnection();
                }
            }
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		} catch (CommunicationException e) {
            connections.release();
            throw e;
        }

		return connection;
	}

	/** {@inheritDoc} */
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

	/**
	 * <p>
	 * Setter for the field <code>host</code>.
	 * </p>
	 * 
	 * @param host
	 *            a {@link java.lang.String} object.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * <p>
	 * Setter for the field <code>port</code>.
	 * </p>
	 * 
	 * @param port
	 *            a int.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/** {@inheritDoc} */
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
