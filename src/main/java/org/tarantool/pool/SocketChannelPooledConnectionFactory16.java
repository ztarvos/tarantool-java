package org.tarantool.pool;

import org.tarantool.msgpack.TarantoolConnection16;
import org.tarantool.msgpack.TarantoolConnection16Impl;

public class SocketChannelPooledConnectionFactory16 extends AbstractSocketChannelPooledConnectionFactory<TarantoolConnection16> {
    public SocketChannelPooledConnectionFactory16(String host, int port, int minPoolSize, int maxPoolSize) {
        super(host, port, minPoolSize, maxPoolSize);
    }

    public SocketChannelPooledConnectionFactory16(int minPoolSize, int maxPoolSize) {
        super(minPoolSize, maxPoolSize);
    }

    public SocketChannelPooledConnectionFactory16() {
    }

    @Override
    public TarantoolConnection16 newUnpooledConnection() {
        return new TarantoolConnection16Impl(host, port);
    }
}
