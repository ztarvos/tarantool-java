package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

/**
 * <p>
 * Ping class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Ping extends Request {
	/** Constant <code>OP_CODE=65280</code> */
	public static final int OP_CODE = 65280;

	/**
	 * <p>
	 * Constructor for Ping.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 */
	public Ping(int id) {
		super(OP_CODE, id);
	}

	/** {@inheritDoc} */
	@Override
	protected int getCapacity() {
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		return buffer;
	}

}
