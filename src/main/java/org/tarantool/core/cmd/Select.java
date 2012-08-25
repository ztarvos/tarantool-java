package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

import org.tarantool.core.Tuple;

public class Select extends Request {
	public static final int OP_CODE = 17;
	int space;
	int index;
	int offset;
	int limit = Integer.MAX_VALUE;
	byte[][] body;

	public Select(int id, byte[]... body) {
		super(OP_CODE, id);
		this.body = body;
	}

	public Select(int id, Tuple... tuples) {
		super(OP_CODE, id);
		body = new byte[tuples.length][];
		for (int i = 0, e = tuples.length; i < e; i++) {
			body[i] = tuples[i].pack();
		}
	}

	public Select space(int space) {
		this.space = space;
		return this;
	}

	public Select index(int index) {
		this.index = index;
		return this;
	}

	public Select offset(int offset) {
		this.offset = offset;
		return this;
	}

	public Select limit(int limit) {
		this.limit = limit;
		return this;
	}

	@Override
	protected int getCapacity() {
		int size = 0;
		for (byte[] b : body) {
			size += b.length;
		}
		return size + 20;
	}

	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		buffer.putInt(space).putInt(index).putInt(offset).putInt(limit).putInt(body.length);
		for (byte[] b : body) {
			buffer.put(b);
		}
		return buffer;
	}

	public int getSpace() {
		return space;
	}

	public void setSpace(int space) {
		this.space = space;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public byte[][] getBody() {
		return body;
	}

	public void setBody(byte[][] body) {
		this.body = body;
	}

}
