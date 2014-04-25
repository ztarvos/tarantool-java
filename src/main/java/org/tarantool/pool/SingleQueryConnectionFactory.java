package org.tarantool.pool;


/**
 * <p>
 * SingleQueryConnectionFactory interface.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public interface SingleQueryConnectionFactory<T> {

	/**
	 * <p>
	 * getSingleQueryConnection.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.core.TarantoolConnection} object.
	 */
	T getSingleQueryConnection();

}
