package org.tarantool.pool;

/**
 * <p>
 * Returnable interface.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public interface Returnable {

	/**
	 * <p>
	 * returnTo.
	 * </p>
	 * 
	 * @param returnPoint
	 *            a {@link org.tarantool.pool.ConnectionReturnPoint} object.
	 */
	void returnTo(ConnectionReturnPoint returnPoint);

}
