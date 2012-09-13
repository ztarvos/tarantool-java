package org.tarantool.facade.chain;

import org.tarantool.facade.Mapping;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Chain<T> {
	protected SingleQueryConnectionFactory factory;
	protected Mapping<T> mapping;

	public Chain(SingleQueryConnectionFactory factory, Mapping<T> mapping) {
		super();
		this.factory = factory;
		this.mapping = mapping;
	}

}
