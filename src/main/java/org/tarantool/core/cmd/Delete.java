package org.tarantool.core.cmd;

import org.tarantool.core.Tuple;

public class Delete extends DMLRequest<Delete> {
	public static final int OP_CODE=21;
	public Delete(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	public Delete(int id, Tuple tuple) {
		super(OP_CODE, id, tuple.pack());
	}

}
