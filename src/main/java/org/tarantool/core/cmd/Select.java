package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

import org.tarantool.core.Tuple;

/**
 * <p>Select class.</p>
 *
 * @author dgreen
 * @version $Id: $
 */
public class Select extends Request {
	/** Constant <code>OP_CODE=17</code> */
	public static final int OP_CODE = 17;
	int space;
	int index;
	int offset;
	int limit = Integer.MAX_VALUE;
	byte[][] body;

	/**
	 * <p>Constructor for Select.</p>
	 *
	 * @param id a int.
	 * @param body an array of byte.
	 */
	public Select(int id, byte[]... body) {
		super(OP_CODE, id);
		this.body = body;
	}

	/**
	 * <p>Constructor for Select.</p>
	 *
	 * @param id a int.
	 * @param tuples a {@link org.tarantool.core.Tuple} object.
	 */
	public Select(int id, Tuple... tuples) {
		super(OP_CODE, id);
		body = new byte[tuples.length][];
		for (int i = 0, e = tuples.length; i < e; i++) {
			body[i] = tuples[i].pack();
		}
	}

	/**
	 * <p>space.</p>
	 *
	 * @param space a int.
	 * @return a {@link org.tarantool.core.cmd.Select} object.
	 */
	public Select space(int space) {
		this.space = space;
		return this;
	}

	/**
	 * <p>index.</p>
	 *
	 * @param index a int.
	 * @return a {@link org.tarantool.core.cmd.Select} object.
	 */
	public Select index(int index) {
		this.index = index;
		return this;
	}

	/**
	 * <p>offset.</p>
	 *
	 * @param offset a int.
	 * @return a {@link org.tarantool.core.cmd.Select} object.
	 */
	public Select offset(int offset) {
		this.offset = offset;
		return this;
	}

	/**
	 * <p>limit.</p>
	 *
	 * @param limit a int.
	 * @return a {@link org.tarantool.core.cmd.Select} object.
	 */
	public Select limit(int limit) {
		this.limit = limit;
		return this;
	}

	/** {@inheritDoc} */
	@Override
	protected int getCapacity() {
		int size = 0;
		for (byte[] b : body) {
			size += b.length;
		}
		return size + 20;
	}

	/** {@inheritDoc} */
	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		buffer.putInt(space).putInt(index).putInt(offset).putInt(limit).putInt(body.length);
		for (byte[] b : body) {
			buffer.put(b);
		}
		return buffer;
	}

	/**
	 * <p>Getter for the field <code>space</code>.</p>
	 *
	 * @return a int.
	 */
	public int getSpace() {
		return space;
	}

	/**
	 * <p>Setter for the field <code>space</code>.</p>
	 *
	 * @param space a int.
	 */
	public void setSpace(int space) {
		this.space = space;
	}

	/**
	 * <p>Getter for the field <code>index</code>.</p>
	 *
	 * @return a int.
	 */
	public int getIndex() {
		return index;
	}

	/**
	 * <p>Setter for the field <code>index</code>.</p>
	 *
	 * @param index a int.
	 */
	public void setIndex(int index) {
		this.index = index;
	}

	/**
	 * <p>Getter for the field <code>offset</code>.</p>
	 *
	 * @return a int.
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * <p>Setter for the field <code>offset</code>.</p>
	 *
	 * @param offset a int.
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * <p>Getter for the field <code>limit</code>.</p>
	 *
	 * @return a int.
	 */
	public int getLimit() {
		return limit;
	}

	/**
	 * <p>Setter for the field <code>limit</code>.</p>
	 *
	 * @param limit a int.
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}

	/**
	 * <p>Getter for the field <code>body</code>.</p>
	 *
	 * @return an array of byte.
	 */
	public byte[][] getBody() {
		return body;
	}

	/**
	 * <p>Setter for the field <code>body</code>.</p>
	 *
	 * @param body an array of byte.
	 */
	public void setBody(byte[][] body) {
		this.body = body;
	}

}
