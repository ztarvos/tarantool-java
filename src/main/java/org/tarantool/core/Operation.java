package org.tarantool.core;

import org.tarantool.core.impl.OperationImpl;
import org.tarantool.core.proto.Updates;

/**
 * Update operations
 *
 * @author dgreen
 * @version $Id: $
 */
public abstract class Operation {

	/**
	 * <p>sub.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation sub(int fieldNo, long value) {
		return new OperationImpl(Updates.SUB, fieldNo, longArg(value));
	}

	/**
	 * <p>sub.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation sub(int fieldNo, int value) {
		return new OperationImpl(Updates.SUB, fieldNo, intArg(value));
	}

	/**
	 * <p>max.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation max(int fieldNo, long value) {
		return new OperationImpl(Updates.MAX, fieldNo, longArg(value));
	}

	/**
	 * <p>max.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation max(int fieldNo, int value) {
		return new OperationImpl(Updates.MAX, fieldNo, intArg(value));
	}

	/**
	 * <p>and.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation and(int fieldNo, int value) {
		return new OperationImpl(Updates.AND, fieldNo, intArg(value));
	}

	/**
	 * <p>and.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation and(int fieldNo, long value) {
		return new OperationImpl(Updates.AND, fieldNo, longArg(value));
	}

	/**
	 * <p>add.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation add(int fieldNo, int value) {
		return new OperationImpl(Updates.ADD, fieldNo, intArg(value));
	}

	/**
	 * <p>add.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation add(int fieldNo, long value) {
		return new OperationImpl(Updates.ADD, fieldNo, longArg(value));
	}

	/**
	 * <p>xor.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation xor(int fieldNo, int value) {
		return new OperationImpl(Updates.XOR, fieldNo, intArg(value));
	}

	/**
	 * <p>xor.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation xor(int fieldNo, long value) {
		return new OperationImpl(Updates.XOR, fieldNo, longArg(value));
	}

	/**
	 * <p>or.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation or(int fieldNo, int value) {
		return new OperationImpl(Updates.OR, fieldNo, intArg(value));
	}

	/**
	 * <p>or.</p>
	 *
	 * @param fieldNo a int.
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation or(int fieldNo, long value) {
		return new OperationImpl(Updates.OR, fieldNo, longArg(value));
	}


	/**
	 * <p>splice.</p>
	 *
	 * @param fieldNo a int.
	 * @param removeFrom a int.
	 * @param removeLength a int.
	 * @param insert an array of byte.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation splice(int fieldNo, int removeFrom, int removeLength, byte[] insert) {
		return new OperationImpl(Updates.SPLICE, fieldNo, new Tuple(3).setInt(0, removeFrom).setInt(1, removeLength).setBytes(2, insert));
	}

	/**
	 * <p>set.</p>
	 *
	 * @param fieldNo a int.
	 * @param args a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation set(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.SET, fieldNo, args);
	}
	
	/**
	 * <p>splice.</p>
	 *
	 * @param fieldNo a int.
	 * @param args a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation splice(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.SPLICE, fieldNo, args);
	}

	/**
	 * <p>delete.</p>
	 *
	 * @param fieldNo a int.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation delete(int fieldNo) {
		return new OperationImpl(Updates.DELETE, fieldNo, intArg(1));
	}

	/**
	 * <p>insert.</p>
	 *
	 * @param fieldNo a int.
	 * @param args a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Operation} object.
	 */
	public static Operation insert(int fieldNo, Tuple args) {
		return new OperationImpl(Updates.INSERT, fieldNo, args);
	}

	/**
	 * <p>longArg.</p>
	 *
	 * @param value a long.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	protected static Tuple longArg(long value) {
		return new Tuple(1).setLong(0, value);
	}

	/**
	 * <p>intArg.</p>
	 *
	 * @param value a int.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	protected static Tuple intArg(int value) {
		return new Tuple(1).setInt(0, value);
	}

	/**
	 * <p>pack.</p>
	 *
	 * @return an array of byte.
	 */
	public abstract byte[] pack();

}
