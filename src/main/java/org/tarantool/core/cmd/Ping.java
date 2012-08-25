package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

public class Ping extends Request {
	public static final int OP_CODE = 65280;

	public Ping(int id) {
		super(OP_CODE, id);
	}

	@Override
	protected int getCapacity() {
		return 0;
	}

	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		return buffer;
	}

}
