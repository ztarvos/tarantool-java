package org.tarantool.core.cmd;

import org.tarantool.core.Tuple;

public class Insert extends DMLRequest<Insert> {
	public static final int OP_CODE=13;
	public Insert(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	public Insert(int id, Tuple tuple) {
		super(OP_CODE, id, tuple.pack());
	}

}
