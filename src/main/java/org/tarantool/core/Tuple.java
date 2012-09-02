package org.tarantool.core;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Date;

import org.tarantool.core.proto.Leb128;

/**
 * Implementing Tarantool tuple
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Tuple {

	byte[][] src;

	/**
	 * <p>
	 * Getter for the field <code>src</code>.
	 * </p>
	 * 
	 * @return an array of byte.
	 */
	public byte[][] getSrc() {
		return src;
	}

	/**
	 * Creates new tuple with specified size
	 * 
	 * @param size
	 *            the size of tuple
	 */
	public Tuple(int size) {
		src = new byte[size][];
	}

	/**
	 * Creates new tuple from binary elements data
	 * 
	 * @param src
	 *            binary representation of tuple elements
	 */
	public Tuple(byte[][] src) {
		this.src = src;
	}

	private ByteBuffer v(int i) {
		return ByteBuffer.wrap(src[i]).order(ByteOrder.LITTLE_ENDIAN);
	}

	private ByteBuffer b(int size) {
		return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
	}

	/**
	 * <p>
	 * size.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int size() {
		return src.length;
	}

	/**
	 * <p>
	 * getInt.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a int.
	 */
	public int getInt(int i) {
		return v(i).getInt();
	}

	/**
	 * <p>
	 * setInt.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a int.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setInt(int i, int value) {
		src[i] = b(4).putInt(value).array();
		return this;
	}

	/**
	 * <p>
	 * getLong.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a long.
	 */
	public long getLong(int i) {
		return v(i).getLong();
	}

	/**
	 * <p>
	 * setLong.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a long.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setLong(int i, long value) {
		src[i] = b(8).putLong(value).array();
		return this;
	}

	/**
	 * <p>
	 * getDouble.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a double.
	 */
	public double getDouble(int i) {
		return v(i).getDouble();
	}

	/**
	 * <p>
	 * setDouble.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a double.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setDouble(int i, double value) {
		src[i] = b(8).putDouble(value).array();
		return this;
	}

	/**
	 * <p>
	 * getFloat.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a float.
	 */
	public float getFloat(int i) {
		return v(i).getFloat();
	}

	/**
	 * <p>
	 * setFloat.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a float.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setFloat(int i, float value) {
		src[i] = b(4).putFloat(value).array();
		return this;
	}

	/**
	 * <p>
	 * getShort.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a short.
	 */
	public short getShort(int i) {
		return v(i).getShort();
	}

	/**
	 * <p>
	 * setShort.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a short.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setShort(int i, short value) {
		src[i] = b(2).putShort(value).array();
		return this;
	}

	/**
	 * <p>
	 * getBigInteger.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a {@link java.math.BigInteger} object.
	 */
	public BigInteger getBigInteger(int i) {
		return new BigInteger(src[i]);
	}

	/**
	 * <p>
	 * setBigInteger.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a {@link java.math.BigInteger} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setBigInteger(int i, BigInteger value) {
		src[i] = value.toByteArray();
		return this;
	}

	/**
	 * <p>
	 * getBigDecimal.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a {@link java.math.BigDecimal} object.
	 */
	public BigDecimal getBigDecimal(int i) {
		int scale = v(i).getInt();
		return new BigDecimal(new BigInteger(Arrays.copyOfRange(src[i], 4, src[i].length)), scale);
	}

	/**
	 * <p>
	 * setBigDecimal.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a {@link java.math.BigDecimal} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setBigDecimal(int i, BigDecimal value) {
		byte[] unscaled = value.unscaledValue().toByteArray();
		src[i] = b(unscaled.length + 4).putInt(value.scale()).put(unscaled).array();
		return this;
	}

	/**
	 * <p>
	 * getDate.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a {@link java.util.Date} object.
	 */
	public Date getDate(int i) {
		return new Date(1000L * getInt(i));
	}

	/**
	 * <p>
	 * setDate.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a {@link java.util.Date} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setDate(int i, Date value) {
		src[i] = b(4).putInt((int) (value.getTime() / 1000L)).array();
		return this;
	}

	/**
	 * <p>
	 * getString.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param encoding
	 *            a {@link java.lang.String} object.
	 * @return a {@link java.lang.String} object.
	 */
	public String getString(int i, String encoding) {
		try {
			return new String(src[i], encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(encoding + " is not supported", e);
		}
	}

	/**
	 * <p>
	 * setString.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a {@link java.lang.String} object.
	 * @param encoding
	 *            a {@link java.lang.String} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setString(int i, String value, String encoding) {
		try {
			src[i] = value.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(encoding + " is not supported", e);
		}
		return this;
	}

	/**
	 * <p>
	 * getBytes.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return an array of byte.
	 */
	public byte[] getBytes(int i) {
		return src[i];
	}

	/**
	 * <p>
	 * setBytes.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param bytes
	 *            an array of byte.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setBytes(int i, byte[] bytes) {
		src[i] = bytes;
		return this;
	}

	/**
	 * <p>
	 * getBoolean.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @return a boolean.
	 */
	public boolean getBoolean(int i) {
		return src[i][0] == 1;
	}

	/**
	 * <p>
	 * setBoolean.
	 * </p>
	 * 
	 * @param i
	 *            a int.
	 * @param value
	 *            a boolean.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple setBoolean(int i, boolean value) {
		src[i] = new byte[] { (value ? (byte) 1 : 0) };
		return this;
	}

	/**
	 * <p>
	 * pack.
	 * </p>
	 * 
	 * @return an array of byte.
	 */
	public byte[] pack() {
		int size = calcFieldsSize();
		ByteBuffer buf = ByteBuffer.allocate(size + 4).order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(size());
		return packFields(buf);
	}

	/**
	 * <p>
	 * packFields.
	 * </p>
	 * 
	 * @param buf
	 *            a {@link java.nio.ByteBuffer} object.
	 * @return an array of byte.
	 */
	public byte[] packFields(ByteBuffer buf) {
		for (byte[] f : src) {
			Leb128.writeUnsigned(buf, f.length).put(f);
		}
		return buf.array();
	}

	/**
	 * <p>
	 * calcFieldsSize.
	 * </p>
	 * 
	 * @return a int.
	 */
	public int calcFieldsSize() {
		int size = 0;
		for (byte[] f : src) {
			size += Leb128.unsignedSize(f.length) + f.length;
		}
		return size;
	}

	/**
	 * <p>
	 * createFQ.
	 * </p>
	 * 
	 * @param buffer
	 *            a {@link java.nio.ByteBuffer} object.
	 * @param order
	 *            a {@link java.nio.ByteOrder} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	@SuppressWarnings("unused")
	public static Tuple createFQ(ByteBuffer buffer, ByteOrder order) {
		int tupleSize = buffer.getInt();
		return create(buffer, order);
	}

	/**
	 * <p>
	 * create.
	 * </p>
	 * 
	 * @param buffer
	 *            a {@link java.nio.ByteBuffer} object.
	 * @param order
	 *            a {@link java.nio.ByteOrder} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public static Tuple create(ByteBuffer buffer, ByteOrder order) {
		int cardinality = buffer.getInt();
		return createFromPackedFields(buffer, order, cardinality);
	}

	/**
	 * <p>
	 * createFromPackedFields.
	 * </p>
	 * 
	 * @param buffer
	 *            a {@link java.nio.ByteBuffer} object.
	 * @param order
	 *            a {@link java.nio.ByteOrder} object.
	 * @param cardinality
	 *            a int.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public static Tuple createFromPackedFields(ByteBuffer buffer, ByteOrder order, int cardinality) {
		byte[][] result = new byte[cardinality][];
		for (int i = 0; i < cardinality; i++) {
			int fieldSize = Leb128.readUnsigned(buffer);
			byte[] field = new byte[fieldSize];
			buffer.get(field);
			result[i] = field;
		}
		return new Tuple(result);
	}

	/** {@inheritDoc} */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(src);
		return result;
	}

	/** {@inheritDoc} */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (!Arrays.deepEquals(src, other.src))
			return false;
		return true;
	}

}
