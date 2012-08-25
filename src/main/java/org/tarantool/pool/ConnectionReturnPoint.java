package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;

public interface ConnectionReturnPoint {
	void returnConnection(TarantoolConnection client);
}
