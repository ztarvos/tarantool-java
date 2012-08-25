package org.tarantool.pool;

public interface Returnable {

	void returnTo(ConnectionReturnPoint returnPoint);

}
