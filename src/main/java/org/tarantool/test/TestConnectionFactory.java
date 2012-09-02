package org.tarantool.test;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.cmd.Transport;
import org.tarantool.core.impl.TarantoolConnectionImpl;
import org.tarantool.pool.SingleQueryConnectionFactory;

/**
 * Connection factory for testing purposes
 */
public class TestConnectionFactory implements SingleQueryConnectionFactory {
	Transport transport;

	public TestConnectionFactory(Transport transport) {
		super();
		this.transport = transport;
	}

	@Override
	public TarantoolConnection getSingleQueryConnection() {
		return new TarantoolConnectionImpl(transport);
	}

}
