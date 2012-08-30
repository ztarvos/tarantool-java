package org.tarantool.pool;

import org.tarantool.core.TarantoolConnection;

/**
 * <p>ConnectionReturnPoint interface.</p>
 *
 * @author dgreen
 * @version $Id: $
 */
public interface ConnectionReturnPoint {
	/**
	 * <p>returnConnection.</p>
	 *
	 * @param client a {@link org.tarantool.core.TarantoolConnection} object.
	 */
	void returnConnection(TarantoolConnection client);
}
