package org.tarantool.core.cmd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import org.tarantool.core.Operation;
import org.tarantool.core.Tuple;

/**
 * <p>Update class.</p>
 *
 * @author dgreen
 * @version $Id: $
 */
public class Update extends DMLRequest<Update> {
	/** Constant <code>OP_CODE=19</code> */
	public static final int OP_CODE = 19;

	/**
	 * <p>Constructor for Update.</p>
	 *
	 * @param id a int.
	 * @param body an array of byte.
	 */
	public Update(int id, byte[] body) {
		super(OP_CODE, id, body);
	}

	/**
	 * <p>Constructor for Update.</p>
	 *
	 * @param id a int.
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @param ops a {@link java.util.List} object.
	 */
	public Update(int id, Tuple tuple, List<Operation> ops) {
		super(OP_CODE, id, null);
		body = packTupleAndOps(tuple, ops);
	}

	/**
	 * <p>packTupleAndOps.</p>
	 *
	 * @param tuple a {@link org.tarantool.core.Tuple} object.
	 * @param ops a {@link java.util.List} object.
	 * @return an array of byte.
	 */
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

	/**
	 * <p>encodeOps.</p>
	 *
	 * @param ops a {@link java.util.List} object.
	 * @return an array of byte.
	 */
	protected byte[][] encodeOps(List<Operation> ops) {
		byte[][] encodedOps = new byte[ops.size()][];
		for (int i = 0; i < encodedOps.length; i++) {
			encodedOps[i] = ops.get(i).pack();
		}
		return encodedOps;
	}

}
