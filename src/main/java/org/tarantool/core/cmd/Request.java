package org.tarantool.core.cmd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Request command base class
 * 
 * @author dgreen
 * @version $Id: $
 */
public abstract class Request {

	protected int op;
	protected int id;
	/** Constant <code>REQUEST_HEADER_SIZE=12</code> */
	protected static final int REQUEST_HEADER_SIZE = 12;

	/**
	 * <p>
	 * Constructor for Request.
	 * </p>
	 * 
	 * @param op
	 *            a int.
	 * @param id
	 *            a int.
	 */
	public Request(int op, int id) {
		super();
		this.op = op;
		this.id = id;
	}

	/**
	 * <p>
	 * getCapacity.
	 * </p>
	 * 
	 * @return a int.
	 */
	protected abstract int getCapacity();

	/**
	 * <p>
	 * getRequestHeaderSize.
	 * </p>
	 * 
	 * @return a int.
	 */
	protected int getRequestHeaderSize() {
		return REQUEST_HEADER_SIZE;
	}

	/**
	 * <p>
	 * body.
	 * </p>
	 * 
	 * @param buffer
	 *            a {@link java.nio.ByteBuffer} object.
	 * @return a {@link java.nio.ByteBuffer} object.
	 */
	public abstract ByteBuffer body(ByteBuffer buffer);

	/**
	 * <p>
	 * pack.
	 * </p>
	 * 
	 * @return a {@link java.nio.ByteBuffer} object.
	 */
	public ByteBuffer pack() {
		int capacity = getCapacity();
		ByteBuffer buffer = body(ByteBuffer.allocate(capacity + getRequestHeaderSize()).order(ByteOrder.LITTLE_ENDIAN).putInt(op).putInt(capacity).putInt(id));
		buffer.flip();
		return buffer;
	}

	/**
	 * <p>
	 * Getter for the field <code>id</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int getId() {
		return id;
	}

	/**
	 * <p>
	 * Setter for the field <code>id</code>.
	 * </p>
	 * 
	 * @param id
	 *            a int.
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * <p>
	 * Getter for the field <code>op</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int getOp() {
		return op;
	}

}
