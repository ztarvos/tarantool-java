package org.tarantool.facade;

import org.tarantool.pool.SingleQueryConnectionFactory;

/**
 * Implements TarantoolTemplate for java 7
 */
public class TarantoolTemplate7 extends TarantoolTemplate {

	public TarantoolTemplate7() {
		super();
	}

	public TarantoolTemplate7(SingleQueryConnectionFactory connectionFactory) {
		super(connectionFactory);
	}

	@Override
	public <T> Mapping<T> getOrCreateMapping(Class<T> cls) {
		@SuppressWarnings("unchecked")
		Mapping<T> m = (Mapping<T>) mapping.get(cls);
		// get mapping is not so expensive, so we can ignore thread safety here
		if (m == null) {
			mapping.put(cls, m = new Mapping7<T>(cls, support));
		}
		return m;
	}

}
