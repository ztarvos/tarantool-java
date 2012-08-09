package org.tarantool.core;

public interface ConnectionReturnPoint {
	void returnConnection(TarantoolClient connection);
}
