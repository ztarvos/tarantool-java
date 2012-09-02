package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

/**
 * <p>
 * Abstract DMLRequest class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public abstract class DMLRequest<T extends DMLRequest<T>> extends Request {
	/**
	 * <p>
	 * Constructor for DMLRequest.
	 * </p>
	 * 
	 * @param op
	 *            a int.
	 * @param id
	 *            a int.
	 * @param body
	 *            an array of byte.
	 */
	public DMLRequest(int op, int id, byte[] body) {
		super(op, id);
		this.body = body;
	}

	int space;
	int flags;
	byte[] body;

	/**
	 * <p>
	 * space.
	 * </p>
	 * 
	 * @param space
	 *            a int.
	 * @return a T object.
	 */
	@SuppressWarnings("unchecked")
	public T space(int space) {
		this.space = space;
		return (T) this;
	}

	/**
	 * <p>
	 * flags.
	 * </p>
	 * 
	 * @param flags
	 *            a int.
	 * @return a T object.
	 */
	@SuppressWarnings("unchecked")
	public T flags(int flags) {
		this.flags = flags;
		return (T) this;
	}

	/**
	 * <p>
	 * space.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int space() {
		return space;
	}

	/**
	 * <p>
	 * flags.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int flags() {
		return flags;
	}

	/** {@inheritDoc} */
	@Override
	protected int getCapacity() {
		return body.length + 8;
	}

	/** {@inheritDoc} */
	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		return buffer.putInt(space).putInt(flags).put(body);
	}

	/**
	 * <p>
	 * Getter for the field <code>space</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int getSpace() {
		return space;
	}

	/**
	 * <p>
	 * Setter for the field <code>space</code>.
	 * </p>
	 * 
	 * @param space
	 *            a int.
	 */
	public void setSpace(int space) {
		this.space = space;
	}

	/**
	 * <p>
	 * Getter for the field <code>flags</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int getFlags() {
		return flags;
	}

	/**
	 * <p>
	 * Setter for the field <code>flags</code>.
	 * </p>
	 * 
	 * @param flags
	 *            a int.
	 */
	public void setFlags(int flags) {
		this.flags = flags;
	}

	/**
	 * <p>
	 * Getter for the field <code>body</code>.
	 * </p>
	 * 
	 * @return an array of byte.
	 */
	public byte[] getBody() {
		return body;
	}

	/**
	 * <p>
	 * Setter for the field <code>body</code>.
	 * </p>
	 * 
	 * @param body
	 *            an array of byte.
	 */
	public void setBody(byte[] body) {
		this.body = body;
	}

}
