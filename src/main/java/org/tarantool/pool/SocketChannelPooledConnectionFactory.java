package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

public class SocketChannelPooledConnectionFactory extends AbstractSocketChannelPooledConnectionFactory<TarantoolConnection> {
    public SocketChannelPooledConnectionFactory(String host, int port, int minPoolSize, int maxPoolSize) {
        super(host, port, minPoolSize, maxPoolSize);
    }

    public SocketChannelPooledConnectionFactory(int minPoolSize, int maxPoolSize) {
        super(minPoolSize, maxPoolSize);
    }

    public SocketChannelPooledConnectionFactory() {
    }

    @Override
    public TarantoolConnection newUnpooledConnection() {
        return new SocketChannelTarantoolConnection(host, port);
    }
}
