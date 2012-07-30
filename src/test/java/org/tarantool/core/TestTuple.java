package org.tarantool.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

import org.junit.Test;
import org.tarantool.core.Tuple;

public class TestTuple {
	int intValue = 2123;
	long longValue = 2321L;

	double doubleValue = 211.23d;
	float floatValue = 123.32f;
	short shortValue = (short) 2178;
	BigInteger bigIntegerValue = BigInteger.valueOf(21212123213321L);
	BigDecimal bigDecimalValue = BigDecimal.valueOf(22121.123);
	Date dateValue = new Date();
	String stringValue = "Hello world!";
	byte[] bytesValue = new byte[] { 1, 2, 3, 4 };

	@Test
	public void testSetGet() {
		Tuple tuple = createTuple();
		checkTuple(tuple);

	}
	
	@Test
	public void testPackUnpack() {
		Tuple tuple = createTuple();
		byte[] packed = tuple.pack();
		ByteBuffer buf = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN);

		checkTuple(Tuple.create(buf,ByteOrder.LITTLE_ENDIAN));

	}

	private void checkTuple(Tuple tuple) {
		assertEquals(intValue, tuple.getInt(0));
		assertEquals(longValue, tuple.getLong(1));
		assertTrue(doubleValue == tuple.getDouble(2));
		assertTrue(floatValue == tuple.getFloat(3));
		assertTrue(shortValue == tuple.getShort(4));
		assertEquals(bigIntegerValue, tuple.getBigInteger(5));
		assertEquals(bigDecimalValue, tuple.getBigDecimal(6));
		assertEquals(dateValue.getTime() / 1000L, tuple.getDate(7).getTime() / 1000L);
		assertArrayEquals(bytesValue, tuple.getBytes(9));
		assertEquals(stringValue, tuple.getString(8, "UTF-8"));
		assertTrue(tuple.getBoolean(10));
	}

	private Tuple createTuple() {
		Tuple tuple = new Tuple(11, ByteOrder.LITTLE_ENDIAN);
		tuple.setInt(0, intValue);
		tuple.setLong(1, longValue);
		tuple.setDouble(2, doubleValue);
		tuple.setFloat(3, floatValue);
		tuple.setShort(4, shortValue);
		tuple.setBigInteger(5, bigIntegerValue);
		tuple.setBigDecimal(6, bigDecimalValue);
		tuple.setDate(7, dateValue);
		tuple.setString(8, stringValue, "UTF-8");
		tuple.setBytes(9, bytesValue);
		tuple.setBoolean(10, true);
		return tuple;
	}

}
