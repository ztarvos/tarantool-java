package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;

/**
 * <p>
 * SingleQueryConnectionFactory interface.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public interface SingleQueryConnectionFactory {

	/**
	 * <p>
	 * getSingleQueryConnection.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.core.TarantoolConnection} object.
	 */
	TarantoolConnection getSingleQueryConnection();

}
