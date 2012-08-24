package org.tarantool.core;

public interface SingleQueryClientFactory {

	TarantoolClient getSingleQueryConnection();

}