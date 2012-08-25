package org.tarantool.core;

import org.tarantool.core.impl.OperationImpl;
import org.tarantool.core.proto.Updates;

/**
 * Update operations
 */
public abstract class Operation {

	public static Operation sub(int fieldNo, long value) {
		return new OperationImpl(Updates.SUB, fieldNo, longArg(value));
	}

	public static Operation sub(int fieldNo, int value) {
		return new OperationImpl(Updates.SUB, fieldNo, intArg(value));
	}

	public static Operation max(int fieldNo, long value) {
		return new OperationImpl(Updates.MAX, fieldNo, longArg(value));
	}

	public static Operation max(int fieldNo, int value) {
		return new OperationImpl(Updates.MAX, fieldNo, intArg(value));
	}

	public static Operation and(int fieldNo, int value) {
		return new OperationImpl(Updates.AND, fieldNo, intArg(value));
	}

	public static Operation and(int fieldNo, long value) {
		return new OperationImpl(Updates.AND, fieldNo, longArg(value));
	}

	public static Operation add(int fieldNo, int value) {
		return new OperationImpl(Updates.ADD, fieldNo, intArg(value));
	}

	public static Operation add(int fieldNo, long value) {
		return new OperationImpl(Updates.ADD, fieldNo, longArg(value));
	}

	public static Operation xor(int fieldNo, int value) {
		return new OperationImpl(Updates.XOR, fieldNo, intArg(value));
	}

	public static Operation xor(int fieldNo, long value) {
		return new OperationImpl(Updates.XOR, fieldNo, longArg(value));
	}

	public static Operation or(int fieldNo, int value) {
		return new OperationImpl(Updates.OR, fieldNo, intArg(value));
	}

	public static Operation or(int fieldNo, long value) {
		return new OperationImpl(Updates.OR, fieldNo, longArg(value));
	}

	

	public static Operation splice(int fieldNo, int removeFrom, int removeLength, byte[] insert) {
		return new OperationImpl(Updates.SPLICE, fieldNo, new Tuple(3).setInt(0, removeFrom).setInt(1, removeLength).setBytes(2, insert));
	}

	public static Operation set(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.SET, fieldNo, args);
	}
	
	public static Operation splice(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.SPLICE, fieldNo, args);
	}

	public static Operation delete(int fieldNo) {
		return new OperationImpl(Updates.DELETE, fieldNo, intArg(1));
	}

	public static Operation insert(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.INSERT, fieldNo, args);
	}

	protected static Tuple longArg(long value) {
		return new Tuple(1).setLong(0, value);
	}

	protected static Tuple intArg(int value) {
		return new Tuple(1).setInt(0, value);
	}

	public abstract byte[] pack();

}
