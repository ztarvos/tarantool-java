package org.tarantool.facade;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.tarantool.facade.chain.Call;
import org.tarantool.facade.chain.ContidionFirst;
import org.tarantool.facade.chain.Delete;
import org.tarantool.facade.chain.Insert;
import org.tarantool.facade.chain.OperationFirst;
import org.tarantool.facade.chain.Search;
import org.tarantool.facade.chain.Update;
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
	 * @return deleted object or null
	 */
	public <T> Delete<T> delete(Class<T> cls, Object... id) {
		return new Delete<T>(connectionFactory, getOrCreateMapping(cls), id);
	}

	/**
	 * Start point of find configuration chain. This method uses primary key to
	 * search.
	 * 
	 * @param cls
	 * @return first element in configuration chain
	 */
	public <T> ContidionFirst<T> find(Class<T> cls) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(connectionFactory, m, 0, m.indexFields(0));
	}

	/**
	 * Start point of find configuration chain. This method will search using
	 * given index. search. Specified fields will be used for type check.
	 * 
	 * @param cls
	 * @return first element in configuration chain
	 */
	public <T> ContidionFirst<T> find(Class<T> cls, int index, String... fields) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(connectionFactory, m, index, fields);
	}

	/**
	 * Start point of find configuration chain. This method will search using
	 * given index. Index fields will be given from index definition inside
	 * mapping.
	 * 
	 * @param cls
	 * @return first element in configuration chain
	 */
	public <T> ContidionFirst<T> find(Class<T> cls, int index) {
		Mapping<T> m = getOrCreateMapping(cls);
		return new Search<T>(connectionFactory, m, index);
	}

	/**
	 * Start point of save configuration chain
	 * 
	 * @param value
	 * @return first element in configuration chain
	 */
	@SuppressWarnings("unchecked")
	public <T> Insert<T> save(T value) {
		return new Insert<T>(connectionFactory, getOrCreateMapping((Class<T>) value.getClass()), value);
	}

	/**
	 * Start point of update configuration chain
	 * 
	 * @param cls
	 * @param id
	 *            primary key of target object
	 * @return first element in configuration chain
	 */
	public <T> OperationFirst<T> update(Class<T> cls, Object... id) {
		return new Update<T>(connectionFactory, getOrCreateMapping(cls), id);
	}

	/**
	 * Creates call configuration chain
	 * 
	 * @param cls
	 *            return type
	 * @param procName
	 *            proc name
	 * @param args
	 *            to pass
	 * @return first element in configuration chain
	 */
	public <T> Call<T> call(Class<T> cls, String procName, Object... args) {
		return new Call<T>(connectionFactory, getOrCreateMapping(cls), procName, args);
	}

	/**
	 * Gets or creates and cache mapping for class from annotations
	 * 
	 * @param cls
	 * @return first element in configuration chain
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

	public SingleQueryConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(SingleQueryConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public TupleSupport getSupport() {
		return support;
	}

	public void setSupport(TupleSupport support) {
		this.support = support;
	}

}
