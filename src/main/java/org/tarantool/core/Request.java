package org.tarantool.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.tarantool.core.Const.OP;

public abstract class Request {

	protected OP op;
	protected int id;
	protected static final int REQUEST_HEADER_SIZE = 12;

	public Request(OP op, int id) {
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
		ByteBuffer buffer = body(ByteBuffer.allocate(capacity + getRequestHeaderSize()).order(ByteOrder.LITTLE_ENDIAN).putInt(op.type).putInt(capacity)
				.putInt(id));
		buffer.flip();
		return buffer;
	}

	public OP getOp() {
		return op;
	}

	public void setOp(OP op) {
		this.op = op;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

}
