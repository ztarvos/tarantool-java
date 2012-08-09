package org.tarantool.core;

public interface SingleQueryClientFactory {

	public abstract TarantoolClient getSingleQueryConnection();

}