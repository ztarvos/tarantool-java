package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

import org.tarantool.core.Const.OP;
import org.tarantool.core.Request;

public class Ping extends Request {

	public Ping(int id) {
		super(OP.PING, id);
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
