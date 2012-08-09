package org.tarantool.core;

import java.util.Collection;
import java.util.List;

public interface TarantoolClient {

	Integer delete(int space, Tuple id);

	Tuple deleteAndGet(int space, Tuple id);

	Tuple updateAndGet(int space, Tuple id, List<Operation> ops);

	Integer update(int space, Tuple id, List<Operation> ops);

	Tuple insertAndGet(int space, Tuple tuple);

	Integer insert(int space, Tuple tuple);

	Integer replace(int space, Tuple tuple);

	Tuple replaceAndGet(int space, Tuple tuple);

	Tuple insertOrReplaceAndGet(int space, Tuple tuple);

	Integer insertOrReplace(int space, Tuple tuple);

	List<Tuple> find(int space, int index, int offset, int limit, Tuple... keys);

	List<Tuple> find(int space, int index, int offset, int limit, Collection<Tuple> keys);

	Tuple findOne(int space, int index, int offset, int limit, Tuple... keys);

	Tuple findOne(int space, int index, int offset, int limit, Collection<Tuple> keys);

	void close();

	Boolean ping();

}