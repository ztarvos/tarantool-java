package org.tarantool.core.cmd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Request command base class
 */
public abstract class Request {

	protected int op;
	protected int id;
	protected static final int REQUEST_HEADER_SIZE = 12;

	public Request(int op, int id) {
		super();
		this.op = op;
		this.id = id;
	}

	protected abstract int getCapacity();

	protected int getRequestHeaderSize() {
		return REQUEST_HEADER_SIZE;
	}

	public abstract ByteBuffer body(ByteBuffer buffer);

	public ByteBuffer pack() {
		int capacity = getCapacity();
		ByteBuffer buffer = body(ByteBuffer.allocate(capacity + getRequestHeaderSize()).order(ByteOrder.LITTLE_ENDIAN).putInt(op).putInt(capacity).putInt(id));
		buffer.flip();
		return buffer;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getOp() {
		return op;
	}

}
