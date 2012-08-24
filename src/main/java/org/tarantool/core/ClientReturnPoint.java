package org.tarantool.core;

public interface ClientReturnPoint {
	void returnConnection(TarantoolClient client);
}
