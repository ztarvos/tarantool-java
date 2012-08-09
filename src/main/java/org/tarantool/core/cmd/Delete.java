package org.tarantool.core.cmd;

import org.tarantool.core.Const.OP;
import org.tarantool.core.Tuple;

public class Delete extends DMLRequest<Delete> {

	public Delete(int id, byte[] body) {
		super(OP.DELETE, id, body);
	}

	public Delete(int id, Tuple tuple) {
		super(OP.DELETE, id, tuple.pack());
	}

}
