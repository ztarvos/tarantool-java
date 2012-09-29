package org.tarantool.facade.chain;

import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Tuple;
import org.tarantool.facade.Mapping;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Search<T> extends Chain<T> implements ContidionFirst<T> {

	public Search(SingleQueryConnectionFactory factory, Mapping<T> mapping, int index, String[] fields) {
		super(factory, mapping);
		this.indexFields = fields;
		this.index = index;
		keys = new ArrayList<Object[]>();
	}

	public Search(SingleQueryConnectionFactory factory, Mapping<T> mapping, int index) {
		super(factory, mapping);
		this.indexFields = mapping.indexFields(index);
		if (this.indexFields == null) {
			throw new IllegalArgumentException("No index defined with id " + index);
		}
		this.index = index;
		keys = new ArrayList<Object[]>();
	}

	List<Object[]> keys;
	int index = 0;
	String[] indexFields;
	int offset = 0;
	int limit = Integer.MAX_VALUE;

	public Search<T> offset(int offset) {
		this.offset = offset;
		return this;
	}

	public Search<T> limit(int limit) {
		this.limit = limit;
		return this;
	}

	@Override
	public Search<T> condition(Object... values) {
		if (indexFields != null) {
			mapping.checkFields(indexFields, values);
		}
		keys.add(values);
		return this;
	}

	public List<T> list() {
		Tuple[] tuples = new Tuple[keys.size()];
		for (int i = 0; i < keys.size(); i++) {
			tuples[i] = mapping.getSupport().create(keys.get(i));
		}
		List<Tuple> response = factory.getSingleQueryConnection().find(mapping.getSpace(), index, offset, limit, tuples);
		List<T> result = new ArrayList<T>();
		for (Tuple tuple : response) {
			result.add(mapping.fromTuple(tuple));
		}
		return result;

	}

	public T one() {
		List<T> list = list();
		return list.isEmpty() ? null : list.get(0);
	}
}