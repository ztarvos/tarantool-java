package org.tarantool.core;

public interface ConnectionReturnPoint {
	void returnConnection(Connection connection);
}
