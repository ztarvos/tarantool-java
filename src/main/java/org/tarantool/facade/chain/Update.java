package org.tarantool.facade.chain;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Operation;
import org.tarantool.core.Tuple;
import org.tarantool.facade.Mapping;
import org.tarantool.facade.TupleSupport;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Update<T> extends Chain<T> implements OperationFirst<T> {
	Object[] id;
	List<Operation> ops = new ArrayList<Operation>();

	public T updateAndGet() {
		return mapping.fromTuple(factory.getSingleQueryConnection().updateAndGet(mapping.getSpace(), id(mapping.getSupport(), id), ops));
	}

	public int update() {
		return factory.getSingleQueryConnection().update(mapping.getSpace(), id(mapping.getSupport(), id), ops);
	}

	public Update(SingleQueryConnectionFactory factory, Mapping<T> mapping, Object... id) {
		super(factory, mapping);
		this.id = id;
	}

	@Override
	public Update<T> add(String name, long value) {
		ops.add(Operation.add(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> add(String name, int value) {
		ops.add(Operation.add(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> and(String name, int value) {
		ops.add(Operation.and(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> and(String name, long value) {
		ops.add(Operation.and(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> or(String name, int value) {
		ops.add(Operation.or(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> or(String name, long value) {
		ops.add(Operation.or(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> xor(String name, int value) {
		ops.add(Operation.xor(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> xor(String name, long value) {
		ops.add(Operation.xor(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> delete(String name) {
		ops.add(Operation.delete(mapping.getFieldNo(name)));
		return this;
	}

	public Update<T> insert(String name, Object value) {
		ops.add(Operation.insert(mapping.getFieldNo(name), mapping.getSupport().create(0)));
		return this;
	}

	@Override
	public Update<T> splice(String name, int offset, int delete, byte[] insert) {
		ops.add(Operation.splice(mapping.getFieldNo(name), mapping.getSupport().create(offset, delete, insert)));
		return this;
	}

	@Override
	public Update<T> splice(String name, String value, int offset, int delete, String insert) {
		int bOffset;
		try {
			bOffset = value.substring(0, offset).getBytes(mapping.getSupport().getEncoding()).length;
			int bLength = value.substring(offset, offset + delete).getBytes(mapping.getSupport().getEncoding()).length;
			ops.add(Operation.splice(mapping.getFieldNo(name),
					mapping.getSupport().create(bOffset, bLength, insert.getBytes(mapping.getSupport().getEncoding()))));
		} catch (UnsupportedEncodingException ignored) {
		}
		return this;
	}

	@Override
	public Update<T> max(String name, long value) {
		ops.add(Operation.max(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> sub(String name, long value) {
		ops.add(Operation.sub(mapping.getFieldNo(name), value));
		return this;
	}

	@Override
	public Update<T> set(String name, Object value) {
		ops.add(Operation.set(mapping.getFieldNo(name), mapping.getSupport().create(value)));
		return this;
	}

	protected Tuple id(TupleSupport support, Object... id) {
		return support.create(id);
	}

}
