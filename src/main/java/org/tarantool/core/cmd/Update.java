package org.tarantool.core.cmd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.tarantool.core.Operation;
import org.tarantool.core.Tuple;

public class Update extends DMLRequest<Update> {
	public static final int OP_CODE = 19;

	public Update(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	public Update(int id, Tuple tuple, List<Operation> ops) {
		super(OP_CODE, id, null);
		body = packTupleAndOps(tuple, ops);
	}

	protected byte[] packTupleAndOps(Tuple tuple, List<Operation> ops) {
		byte[] packedTuple = tuple.pack();
		byte[][] encodedOps = encodeOps(ops);
		int count = ops == null ? 0 : ops.size();
		int capacity = packedTuple.length + 4;
		if (ops != null)
			for (byte[] op : encodedOps) {
				capacity += op.length;
			}
		ByteBuffer body = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
		body.put(packedTuple);
		body.putInt(count);
		if (ops != null) {
			for (byte[] op : encodedOps) {
				body.put(op);
			}
		}
		return body.array();
	}

	protected byte[][] encodeOps(List<Operation> ops) {
		byte[][] encodedOps = new byte[ops.size()][];
		for (int i = 0; i < encodedOps.length; i++) {
			encodedOps[i] = ops.get(i).pack();
		}
		return encodedOps;
	}

}
