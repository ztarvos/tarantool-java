package org.tarantool.facade.chain;

import org.tarantool.facade.Mapping;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Insert<T> extends Chain<T> {
	T value;

	public Insert(SingleQueryConnectionFactory factory, Mapping<T> mapping, T value) {
		super(factory, mapping);
		this.mapping = mapping;
		this.value = value;
	}

	public int insert() {
		return factory.getSingleQueryConnection().insert(mapping.getSpace(), mapping.toTuple(value));
	}

	public int replace() {
		return factory.getSingleQueryConnection().replace(mapping.getSpace(), mapping.toTuple(value));
	}

	public int insertOrReplace() {
		return factory.getSingleQueryConnection().insertOrReplace(mapping.getSpace(), mapping.toTuple(value));
	}

	public T insertOrReplaceAndGet() {
		return mapping.fromTuple(factory.getSingleQueryConnection().insertOrReplaceAndGet(mapping.getSpace(), mapping.toTuple(value)));
	}

	public T insertAndGet() {
		return mapping.fromTuple(factory.getSingleQueryConnection().insertAndGet(mapping.getSpace(), mapping.toTuple(value)));
	}

	public T replaceAndGet() {
		return mapping.fromTuple(factory.getSingleQueryConnection().replaceAndGet(mapping.getSpace(), mapping.toTuple(value)));
	}
}
