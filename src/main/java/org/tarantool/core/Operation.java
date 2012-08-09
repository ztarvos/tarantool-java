package org.tarantool.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.tarantool.core.Const.UP;

public class Operation {
	UP op;
	int fieldNo;
	Tuple args;

	public Operation(UP op, int fieldNo, Tuple args) {
		super();
		this.op = op;
		this.fieldNo = fieldNo;
		this.args = args;
		if (args.size() != op.args) {
			throw new IllegalArgumentException(op.name() + " required " + op.args + " arguments but has " + args.size());
		}
	}

	public UP getOp() {
		return op;
	}

	public int getFieldNo() {
		return fieldNo;
	}

	public byte[] pack() {
		int fieldsSize = args.calcFieldsSize();
		int multiFieldOffset = args.size() > 1 ? Leb128.unsignedSize(fieldsSize) : 0;
		ByteBuffer buf = ByteBuffer.allocate(5 + fieldsSize + multiFieldOffset).order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(fieldNo);
		buf.put((byte) op.type);
		if (args.size() > 1) {
			Leb128.writeUnsigned(buf, fieldsSize);
		}
		return args.packFields(buf);
	}

}
