package org.tarantool.facade.chain;

import org.tarantool.core.Tuple;
import org.tarantool.facade.Mapping;
import org.tarantool.facade.TupleSupport;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Delete<T> extends Chain<T> {
	Object[] id;
	Mapping<T> mapping;

	public Delete(SingleQueryConnectionFactory factory, Mapping<T> mapping, Object... id) {
		super(factory, mapping);
		this.id = id;
		this.mapping = mapping;
	}

	public int delete() {
		return factory.getSingleQueryConnection().delete(mapping.getSpace(), id(mapping.getSupport(), id));
	}

	public T deleteAndGet() {
		return mapping.fromTuple(factory.getSingleQueryConnection().deleteAndGet(mapping.getSpace(), id(mapping.getSupport(), id)));
	}

	protected Tuple id(TupleSupport support, Object... id) {
		return support.create(id);
	}
}
