package org.tarantool.core.proto;

/**
 * <p>Flags interface.</p>
 *
 * @author dgreen
 * @version $Id: $
 */
public interface Flags {
	/**
	 * DML Request flags
	 */
	static final int RETURN_TUPLE = 0x01;
	/** Constant <code>ADD_TUPLE=0x02</code> */
	static final int ADD_TUPLE = 0x02;
	/** Constant <code>REPLACE_TUPLE=0x04</code> */
	static final int REPLACE_TUPLE = 0x04;

}
