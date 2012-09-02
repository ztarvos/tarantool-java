package org.tarantool.core.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.tarantool.core.Operation;
import org.tarantool.core.Tuple;
import org.tarantool.core.proto.Leb128;
import org.tarantool.core.proto.Updates;

/**
 * <p>
 * OperationImpl class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class OperationImpl extends Operation {
	/**
	 * Creates update operation
	 * 
	 * @param op
	 *            Update operation {@link org.tarantool.core.proto.Updates}
	 * @param fieldNo
	 *            Number of field to execute update. Remember that Tarantool
	 *            supports only 3 data types: integer, long and byte[]. So
	 *            arithmetic operations can only be executed on integer and long
	 *            elements.
	 * @param args
	 *            Operation arguments. usually there should be 1 argument, but
	 *            SPLICE has 3
	 */
	public OperationImpl(Updates op, int fieldNo, Tuple args) {
		this.op = op;
		this.fieldNo = fieldNo;
		this.args = args;
		if (args.size() != op.args) {
			throw new IllegalArgumentException(op.name() + " required " + op.args + " arguments but has " + args.size());
		}
	}

	Updates op;
	int fieldNo;
	Tuple args;

	/**
	 * <p>
	 * Getter for the field <code>op</code>.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.core.proto.Updates} object.
	 */
	public Updates getOp() {
		return op;
	}

	/**
	 * <p>
	 * Setter for the field <code>op</code>.
	 * </p>
	 * 
	 * @param op
	 *            a {@link org.tarantool.core.proto.Updates} object.
	 */
	public void setOp(Updates op) {
		this.op = op;
	}

	/**
	 * <p>
	 * Getter for the field <code>fieldNo</code>.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int getFieldNo() {
		return fieldNo;
	}

	/**
	 * <p>
	 * Setter for the field <code>fieldNo</code>.
	 * </p>
	 * 
	 * @param fieldNo
	 *            a int.
	 */
	public void setFieldNo(int fieldNo) {
		this.fieldNo = fieldNo;
	}

	/**
	 * <p>
	 * Getter for the field <code>args</code>.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple getArgs() {
		return args;
	}

	/**
	 * <p>
	 * Setter for the field <code>args</code>.
	 * </p>
	 * 
	 * @param args
	 *            a {@link org.tarantool.core.Tuple} object.
	 */
	public void setArgs(Tuple args) {
		this.args = args;
	}

	/**
	 * <p>
	 * pack.
	 * </p>
	 * 
	 * @return an array of byte.
	 */
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
