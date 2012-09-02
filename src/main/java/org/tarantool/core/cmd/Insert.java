package org.tarantool.core.cmd;

import org.tarantool.core.Tuple;

/**
 * <p>
 * Insert class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Insert extends DMLRequest<Insert> {
	/** Constant <code>OP_CODE=13</code> */
	public static final int OP_CODE = 13;

	/**
	 * <p>
	 * Constructor for Insert.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 * @param body
	 *            an array of byte.
	 */
	public Insert(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	/**
	 * <p>
	 * Constructor for Insert.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 */
	public Insert(int id, Tuple tuple) {
		super(OP_CODE, id, tuple.pack());
	}

}
