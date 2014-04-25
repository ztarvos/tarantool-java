package org.tarantool.pool;


/**
 * <p>
 * ConnectionReturnPoint interface.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public interface ConnectionReturnPoint<T> {
	/**
	 * <p>
	 * returnConnection.
	 * </p>
	 * 
	 * @param client
	 *            a {@link org.tarantool.core.TarantoolConnection} object.
	 */
	void returnConnection(T client);
}
