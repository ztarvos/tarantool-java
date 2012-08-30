package org.tarantool.core;

import java.util.Collection;
import java.util.List;

/**
 * A connection with a specific instance of tarantool
 *
 * @author dgreen
 * @version $Id: $
 */
public interface TarantoolConnection {

	/**
	 * Finds tuple using primary key and removes it
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param id
	 *            tuple which contains only primary key
	 * @return tuples affected
	 */
	Integer delete(int space, Tuple id);

	/**
	 * Finds tuple using primary key and removes it
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param id
	 *            tuple which contains only primary key
	 * @return affected tuple or null
	 */
	Tuple deleteAndGet(int space, Tuple id);

	/**
	 * Finds tuple using primary key and performs specified update operations
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param id
	 *            tuple which contains only primary key
	 * @param ops
	 *            update operations Operation
	 * @return affected tuple or null
	 */
	Tuple updateAndGet(int space, Tuple id, List<Operation> ops);

	/**
	 * Finds tuple using primary key and performs specified update operations
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param id
	 *            tuple which contains only primary key
	 * @param ops
	 *            update operations {@link org.tarantool.core.Operation}
	 * @return tuples affected
	 */
	Integer update(int space, Tuple id, List<Operation> ops);

	/**
	 * Inserts specified tuple or throws {@link org.tarantool.core.exception.TarantoolException} if tuple
	 * already exists
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuple inserted
	 */
	Tuple insertAndGet(int space, Tuple tuple);

	/**
	 * Inserts specified tuple or throws {@link org.tarantool.core.exception.TarantoolException} if tuple
	 * already exists
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuples inserted
	 */
	Integer insert(int space, Tuple tuple);

	/**
	 * Replaces specified tuple or throws {@link org.tarantool.core.exception.TarantoolException} if no tuple
	 * with the same primary key found
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuples replaced
	 */
	Integer replace(int space, Tuple tuple);

	/**
	 * Replaces specified tuple or throws {@link org.tarantool.core.exception.TarantoolException} if no tuple
	 * with the same primary key found
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuple replaced
	 */
	Tuple replaceAndGet(int space, Tuple tuple);

	/**
	 * Inserts or Replaces specified tuple
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuple affected
	 */
	Tuple insertOrReplaceAndGet(int space, Tuple tuple);

	/**
	 * Inserts or Replaces specified tuple
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @return tuples affected
	 */
	Integer insertOrReplace(int space, Tuple tuple);

	/**
	 * Finds elements using selected index and matches them against specified
	 * keys
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param index
	 *            Index to use. Should be configured in tarantool.cfg
	 * @param offset a int.
	 * @param limit a int.
	 * @param keys
	 *            Keys to match. If a tuple matches more than one key, it's
	 *            returned twice.
	 * @return list of tuples matched
	 */
	List<Tuple> find(int space, int index, int offset, int limit, Tuple... keys);

	/**
	 * Finds elements using selected index and matches them against specified
	 * keys
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param index
	 *            Index to use. Should be configured in tarantool.cfg
	 * @param offset a int.
	 * @param limit a int.
	 * @param keys
	 *            Keys to match. If a tuple matches more than one key, it's
	 *            returned twice.
	 * @return list of tuples matched
	 */
	List<Tuple> find(int space, int index, int offset, int limit, Collection<Tuple> keys);

	/**
	 * Finds first element using selected index and matches them against
	 * specified keys
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param index
	 *            Index to use. Should be configured in tarantool.cfg
	 * @param offset a int.
	 * @param keys
	 *            Keys to match.
	 * @return keys matched
	 */
	Tuple findOne(int space, int index, int offset, Tuple... keys);

	/**
	 * Finds first element using selected index and matches them against
	 * specified keys
	 *
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param index
	 *            Index to use. Should be configured in tarantool.cfg
	 * @param offset a int.
	 * @param keys
	 *            Keys to match.
	 * @return keys matched
	 */
	Tuple findOne(int space, int index, int offset, Collection<Tuple> keys);

	/**
	 * Closes connection
	 */
	void close();

	/**
	 * Pings server
	 *
	 * @return true of throws {@link org.tarantool.core.exception.CommunicationException}
	 */
	Boolean ping();

}
