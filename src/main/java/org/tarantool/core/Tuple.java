package org.tarantool.core;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Date;

public class Tuple {

	byte[][] src;
	ByteOrder order;

	public byte[][] getSrc() {
		return src;
	}

	public Tuple(int size, ByteOrder order) {
		src = new byte[size][];
		this.order = order;
	}

	public Tuple(byte[][] src, ByteOrder order) {
		this.src = src;
		this.order = order;
	}

	private ByteBuffer v(int i) {
		return ByteBuffer.wrap(src[i]).order(order);
	}

	private ByteBuffer b(int size) {
		return ByteBuffer.allocate(size).order(order);
	}

	public int size() {
		return src.length;
	}

	public int getInt(int i) {
		return v(i).getInt();
	}

	public Tuple setInt(int i, int value) {
		src[i] = b(4).putInt(value).array();
		return this;
	}

	public long getLong(int i) {
		return v(i).getLong();
	}

	public Tuple setLong(int i, long value) {
		src[i] = b(8).putLong(value).array();
		return this;
	}

	public double getDouble(int i) {
		return v(i).getDouble();
	}

	public Tuple setDouble(int i, double value) {
		src[i] = b(8).putDouble(value).array();
		return this;
	}

	public float getFloat(int i) {
		return v(i).getFloat();
	}

	public Tuple setFloat(int i, float value) {
		src[i] = b(4).putFloat(value).array();
		return this;
	}

	public short getShort(int i) {
		return v(i).getShort();
	}

	public Tuple setShort(int i, short value) {
		src[i] = b(2).putShort(value).array();
		return this;
	}

	public BigInteger getBigInteger(int i) {
		return new BigInteger(src[i]);
	}

	public Tuple setBigInteger(int i, BigInteger value) {
		src[i] = value.toByteArray();
		return this;
	}

	public BigDecimal getBigDecimal(int i) {
		int scale = v(i).getInt();
		return new BigDecimal(new BigInteger(Arrays.copyOfRange(src[i], 4, src[i].length)), scale);
	}

	public Tuple setBigDecimal(int i, BigDecimal value) {
		byte[] unscaled = value.unscaledValue().toByteArray();
		src[i] = b(unscaled.length + 4).putInt(value.scale()).put(unscaled).array();
		return this;
	}

	public Date getDate(int i) {
		return new Date(1000L * getInt(i));
	}

	public Tuple setDate(int i, Date value) {
		src[i] = b(4).putInt((int) (value.getTime() / 1000L)).array();
		return this;
	}

	public String getString(int i, String encoding) {
		try {
			return new String(src[i], encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(encoding + " is not supported", e);
		}
	}

	public Tuple setString(int i, String value, String encoding) {
		try {
			src[i] = value.getBytes(encoding);
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(encoding + " is not supported", e);
		}
		return this;
	}

	public byte[] getBytes(int i) {
		return src[i];
	}

	public Tuple setBytes(int i, byte[] bytes) {
		src[i] = bytes;
		return this;
	}

	public boolean getBoolean(int i) {
		return src[i][0] == 1;
	}

	public Tuple setBoolean(int i, boolean value) {
		src[i] = new byte[] { (value ? (byte) 1 : 0) };
		return this;
	}

	public byte[] pack() {
		int size = calcFieldsSize();
		ByteBuffer buf = ByteBuffer.allocate(size + 4).order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(size());
		return packFields(buf);
	}

	public byte[] packFields(ByteBuffer buf) {
		for (byte[] f : src) {
			Leb128.writeUnsigned(buf, f.length).put(f);
		}
		return buf.array();
	}

	public int calcFieldsSize() {
		int size = 0;
		for (byte[] f : src) {
			size += Leb128.unsignedSize(f.length) + f.length;
		}
		return size;
	}

	@SuppressWarnings("unused")
	public static Tuple createFQ(ByteBuffer buffer, ByteOrder order) {
		int tupleSize = buffer.getInt();
		return create(buffer, order);
	}

	public static Tuple create(ByteBuffer buffer, ByteOrder order) {
		int cardinality = buffer.getInt();
		return createFromPackedFields(buffer, order, cardinality);
	}

	public static Tuple createFromPackedFields(ByteBuffer buffer, ByteOrder order, int cardinality) {
		byte[][] result = new byte[cardinality][];
		for (int i = 0; i < cardinality; i++) {
			int fieldSize = Leb128.readUnsigned(buffer);
			byte[] field = new byte[fieldSize];
			buffer.get(field);
			result[i] = field;
		}
		return new Tuple(result, order);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((order == null) ? 0 : order.hashCode());
		result = prime * result + Arrays.hashCode(src);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (order == null) {
			if (other.order != null)
				return false;
		} else if (!order.equals(other.order))
			return false;
		if (!Arrays.deepEquals(src, other.src))
			return false;
		return true;
	}

}
