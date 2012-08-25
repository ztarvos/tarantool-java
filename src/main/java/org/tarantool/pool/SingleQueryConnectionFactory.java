package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;

public interface SingleQueryConnectionFactory {

	TarantoolConnection getSingleQueryConnection();

}