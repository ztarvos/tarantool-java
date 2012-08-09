package org.tarantool.core.cmd;

import org.tarantool.core.Const.OP;
import org.tarantool.core.Tuple;

public class Insert extends DMLRequest<Insert> {

	public Insert(int id, byte[] body) {
		super(OP.INSERT, id, body);
	}

	public Insert(int id, Tuple tuple) {
		super(OP.INSERT, id, tuple.pack());
	}

}
