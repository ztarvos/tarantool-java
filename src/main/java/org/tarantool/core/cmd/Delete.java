package org.tarantool.core.cmd;

import org.tarantool.core.Tuple;

/**
 * <p>
 * Delete class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Delete extends DMLRequest<Delete> {
	/** Constant <code>OP_CODE=21</code> */
	public static final int OP_CODE = 21;

	/**
	 * <p>
	 * Constructor for Delete.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 * @param body
	 *            an array of byte.
	 */
	public Delete(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	/**
	 * <p>
	 * Constructor for Delete.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 */
	public Delete(int id, Tuple tuple) {
		super(OP_CODE, id, tuple.pack());
	}

}
