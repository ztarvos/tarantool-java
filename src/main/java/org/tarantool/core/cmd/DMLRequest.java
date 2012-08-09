package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

import org.tarantool.core.Const.OP;
import org.tarantool.core.Request;

public abstract class DMLRequest<T extends DMLRequest<T>> extends Request {
	public DMLRequest(OP op, int id, byte[] body) {
		super(op, id);
		this.body = body;
	}

	int space;
	int flags;
	byte[] body;

	@SuppressWarnings("unchecked")
	public T space(int space) {
		this.space = space;
		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T flags(int flags) {
		this.flags = flags;
		return (T) this;
	}

	public int space() {
		return space;
	}

	public int flags() {
		return flags;
	}

	@Override
	protected int getCapacity() {
		return body.length + 8;
	}

	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		return buffer.putInt(space).putInt(flags).put(body);
	}

	public int getSpace() {
		return space;
	}

	public void setSpace(int space) {
		this.space = space;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

}
