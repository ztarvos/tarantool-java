package org.tarantool.core;

import java.util.Collection;
import java.util.List;

import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.exception.TarantoolException;

/**
 * A connection with a specific instance of tarantool
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
	 *            update operations {@link Operation}
	 * @return tuples affected
	 */
	Integer update(int space, Tuple id, List<Operation> ops);

	/**
	 * Inserts specified tuple or throws {@link TarantoolException} if tuple
	 * already exists
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
	 * @return tuple inserted
	 */
	Tuple insertAndGet(int space, Tuple tuple);

	/**
	 * Inserts specified tuple or throws {@link TarantoolException} if tuple
	 * already exists
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
	 * @return tuples inserted
	 */
	Integer insert(int space, Tuple tuple);

	/**
	 * Replaces specified tuple or throws {@link TarantoolException} if no tuple
	 * with the same primary key found
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
	 * @return tuples replaced
	 */
	Integer replace(int space, Tuple tuple);

	/**
	 * Replaces specified tuple or throws {@link TarantoolException} if no tuple
	 * with the same primary key found
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
	 * @return tuple replaced
	 */

	Tuple replaceAndGet(int space, Tuple tuple);

	/**
	 * Inserts or Replaces specified tuple
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
	 * @return tuple affected
	 */
	Tuple insertOrReplaceAndGet(int space, Tuple tuple);

	/**
	 * Inserts or Replaces specified tuple
	 * 
	 * @param space
	 *            Space to query. Should be configured in tarantool.cfg
	 * @param tuple
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
	 * @param offset
	 * @param limit
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
	 * @param offset
	 * @param limit
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
	 * @param offset
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
	 * @param offset
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
	 * @return true of throws {@link CommunicationException}
	 */
	Boolean ping();

}