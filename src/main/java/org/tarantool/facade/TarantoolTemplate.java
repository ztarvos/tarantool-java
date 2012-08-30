package org.tarantool.facade;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.tarantool.core.Operation;
import org.tarantool.core.Tuple;
import org.tarantool.pool.SingleQueryConnectionFactory;

/**
 * Simplifies the use of Tarantool.
 * 
 */
public class TarantoolTemplate {
	protected Map<Class<?>, Mapping<?>> mapping = new ConcurrentHashMap<Class<?>, Mapping<?>>();
	protected SingleQueryConnectionFactory connectionFactory;
	protected TupleSupport support = new TupleSupport();

	/**
	 * Creates new TarantoolTemplate. You should specify connectionFactory
	 * before starts.
	 */
	public TarantoolTemplate() {
		super();
	}

	/**
	 * Creates new TarantoolTemplate.
	 * 
	 * @param connectionFactory
	 */
	public TarantoolTemplate(SingleQueryConnectionFactory connectionFactory) {
		super();
		this.connectionFactory = connectionFactory;
	}

	public void addMapping(Mapping<?> mapping) {
		this.mapping.put(mapping.getMappedClass(), mapping);
	}

	/**
	 * Start point of delete configuration chain
	 * 
	 * @param cls
	 * @param id
	 * @return
	 */
	public <T> Delete<T> delete(Class<T> cls, Object... id) {
		return new Delete<T>(getOrCreateMapping(cls), id);
	}

	/**
	 * Start point of find configuration chain. This method uses primary key to
	 * search.
	 * 
	 * @param cls
	 * @return
	 */
	public <T> ContidionFirst<T> find(Class<T> cls) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(m, 0, m.indexFields(0));
	}

	/**
	 * Start point of find configuration chain. This method will search using
	 * given index. search. Specified fields will be used for type check.
	 * 
	 * @param cls
	 * @return
	 */
	public <T> ContidionFirst<T> find(Class<T> cls, int index, String... fields) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(m, index, fields);
	}

	/**
	 * Start point of find configuration chain. This method will search using
	 * given index. Index fields will be given from index definition inside
	 * mapping.
	 * 
	 * @param cls
	 * @return
	 */
	public <T> ContidionFirst<T> find(Class<T> cls, int index) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(m, index);
	}

	/**
	 * Start point of save configuration chain
	 * 
	 * @param value
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> Insert<T> save(T value) {
		return new Insert<T>(getOrCreateMapping((Class<T>) value.getClass()), value);
	}

	/**
	 * Start point of update configuration chain
	 * 
	 * @param cls
	 * @param id
	 *            primary key of target object
	 * @return
	 */
	public <T> OperationFirst<T> update(Class<T> cls, Object... id) {
		return new Update<T>(getOrCreateMapping(cls), id);
	}

	/**
	 * Gets or creates and cache mapping for class from annotations
	 * 
	 * @param cls
	 * @return
	 */
	public <T> Mapping<T> getOrCreateMapping(Class<T> cls) {
		@SuppressWarnings("unchecked")
		Mapping<T> m = (Mapping<T>) mapping.get(cls);
		// get mapping is not so expensive, so we can ignore thread safety here
		if (m == null) {
			mapping.put(cls, m = new Mapping<T>(cls, support));
		}
		return m;
	}

	public abstract class ContidionFirst<T> {
		public abstract Search<T> condition(Object... values);
	}

	public class Search<T> extends ContidionFirst<T> {
		Mapping<T> mapping;

		public Search(Mapping<T> mapping, int index, String[] fields) {
			this.mapping = mapping;
			this.indexFields = fields;
			this.index = index;
			keys = new ArrayList<Object[]>();
		}

		public Search(Mapping<T> mapping, int index) {
			this.mapping = mapping;
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
			List<Tuple> response = connectionFactory.getSingleQueryConnection().find(mapping.getSpace(), index, offset, limit, tuples);
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

	public class Insert<T> {
		T value;
		Mapping<T> mapping;

		public Insert(Mapping<T> mapping, T value) {
			this.mapping = mapping;
			this.value = value;
		}

		public int insert() {
			return connectionFactory.getSingleQueryConnection().insert(mapping.getSpace(), mapping.toTuple(value));
		}

		public int replace() {
			return connectionFactory.getSingleQueryConnection().replace(mapping.getSpace(), mapping.toTuple(value));
		}

		public int insertOrReplace() {
			return connectionFactory.getSingleQueryConnection().insertOrReplace(mapping.getSpace(), mapping.toTuple(value));
		}

		public T insertOrReplaceAndGet() {
			return mapping.fromTuple(connectionFactory.getSingleQueryConnection().insertOrReplaceAndGet(mapping.getSpace(), mapping.toTuple(value)));
		}

		public T insertAndGet() {
			return mapping.fromTuple(connectionFactory.getSingleQueryConnection().insertAndGet(mapping.getSpace(), mapping.toTuple(value)));
		}

		public T replaceAndGet() {
			return mapping.fromTuple(connectionFactory.getSingleQueryConnection().replaceAndGet(mapping.getSpace(), mapping.toTuple(value)));
		}
	}

	public class Delete<T> {
		Object[] id;
		Mapping<T> mapping;

		private Delete(Mapping<T> mapping, Object... id) {
			super();
			this.id = id;
			this.mapping = mapping;
		}

		public int delete() {
			return connectionFactory.getSingleQueryConnection().delete(mapping.getSpace(), id(mapping.getSupport(), id));
		}

		public T deleteAndGet() {
			return mapping.fromTuple(connectionFactory.getSingleQueryConnection().deleteAndGet(mapping.getSpace(), id(mapping.getSupport(), id)));
		}
	}

	public abstract class OperationFirst<T> {
		public abstract Update<T> add(String name, long value);

		public abstract Update<T> max(String name, long value);

		public abstract Update<T> sub(String name, long value);

		public abstract Update<T> add(String name, int value);

		public abstract Update<T> and(String name, int value);

		public abstract Update<T> and(String name, long value);

		public abstract Update<T> or(String name, int value);

		public abstract Update<T> or(String name, long value);

		public abstract Update<T> xor(String name, int value);

		public abstract Update<T> xor(String name, long value);

		public abstract Update<T> delete(String name);

		public abstract Update<T> insert(String name, Object value);

		public abstract Update<T> set(String name, Object value);

		public abstract Update<T> splice(String name, int offset, int delete, byte[] insert);

		public abstract Update<T> splice(String name, String value, int offset, int delete, String insert);
	}

	public class Update<T> extends OperationFirst<T> {
		Object[] id;
		Mapping<T> mapping;

		List<Operation> ops = new ArrayList<Operation>();

		public T updateAndGet() {
			return mapping.fromTuple(connectionFactory.getSingleQueryConnection().updateAndGet(mapping.getSpace(), id(mapping.getSupport(), id), ops));
		}

		public int update() {
			return connectionFactory.getSingleQueryConnection().update(mapping.getSpace(), id(mapping.getSupport(), id), ops);
		}

		private Update(Mapping<T> mapping, Object... id) {
			super();
			this.id = id;
			this.mapping = mapping;
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
			ops.add(Operation.splice(mapping.getFieldNo(name), mapping.support.create(offset, delete, insert)));
			return this;
		}

		@Override
		public Update<T> splice(String name, String value, int offset, int delete, String insert) {
			int bOffset;
			try {
				bOffset = value.substring(0, offset).getBytes(mapping.support.encoding).length;
				int bLength = value.substring(offset, offset + delete).getBytes(mapping.support.encoding).length;
				ops.add(Operation.splice(mapping.getFieldNo(name), mapping.support.create(bOffset, bLength, insert.getBytes(mapping.support.encoding))));
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
			ops.add(Operation.set(mapping.getFieldNo(name), mapping.support.create(value)));
			return this;
		}

	}

	public SingleQueryConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(SingleQueryConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	private Tuple id(TupleSupport support, Object... id) {
		return support.create(id);
	}

	public TupleSupport getSupport() {
		return support;
	}

	public void setSupport(TupleSupport support) {
		this.support = support;
	}

}
