package org.tarantool.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.tarantool.core.Const.UP;
import org.tarantool.core.exception.TarantoolException;

public class StandardTest {
	public static int PRIM_AND_SEC_SPACE = 123;
	public static int COMPOSITE_SPACE = 124;
	Integer ONE = Integer.valueOf(1);
	Tuple a;
	Tuple b;
	Tuple c;
	Tuple d;

	Tuple aId;
	Tuple bId;
	Tuple cId;
	Tuple dId;

	Tuple eId;
	Tuple secKeyA;

	Tuple secKeyB;

	Tuple updateNameA;

	Tuple returnNameA;

	Tuple incrementAccessA;

	Tuple empty = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 0);

	Tuple addMarkA;

	Tuple a2;
	Tuple b2;

	Tuple a2Id;

	TarantoolClient connection;

	public StandardTest(TarantoolClient connection) {
		super();
		this.connection = connection;
		init();
	}

	public void run() {
		testBase();
		testCompositePK();
	}

	public void init() {
		a = new Tuple(5, ByteOrder.LITTLE_ENDIAN).setInt(0, 1).setString(1, "Microsoft", "UTF-8").setInt(2, 1).setString(3, "John Smith", "UTF-8")
				.setBigDecimal(4, new BigDecimal(987654.0));
		b = new Tuple(5, ByteOrder.LITTLE_ENDIAN).setInt(0, 2).setString(1, "Microsoft", "UTF-8").setInt(2, 1).setString(3, "Dave Jones", "UTF-8")
				.setBigDecimal(4, new BigDecimal(87654.21));
		c = new Tuple(5, ByteOrder.LITTLE_ENDIAN).setInt(0, 3).setString(1, "Adobe", "UTF-8").setInt(2, 3).setString(3, "Ben James", "UTF-8")
				.setBigDecimal(4, new BigDecimal(97654.312));
		d = new Tuple(5, ByteOrder.LITTLE_ENDIAN).setInt(0, 4).setString(1, "Microsoft", "UTF-8").setInt(2, 2).setString(3, "Bill Smith", "UTF-8")
				.setBigDecimal(4, new BigDecimal(98764.213));
		aId = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 1);
		bId = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 2);
		cId = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 3);
		dId = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 4);

		eId = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 5);

		secKeyA = new Tuple(2, ByteOrder.LITTLE_ENDIAN).setString(0, "Microsoft", "UTF-8").setInt(1, 1);

		secKeyB = new Tuple(2, ByteOrder.LITTLE_ENDIAN).setString(0, "Microsoft", "UTF-8").setInt(1, 2);

		updateNameA = new Tuple(3, ByteOrder.LITTLE_ENDIAN).setInt(0, 0).setInt(1, 4).setString(2, "Johny", "UTF-8");

		returnNameA = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setString(0, "John Smith", "UTF-8");

		incrementAccessA = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 1);

		addMarkA = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setString(0, "[FIRED]", "UTF-8");

		a2 = new Tuple(4, ByteOrder.LITTLE_ENDIAN).setInt(0, 1).setInt(1, 1).setInt(2, 22).setString(3, "John Smith", "UTF-8");

		b2 = new Tuple(4, ByteOrder.LITTLE_ENDIAN).setInt(0, 2).setInt(1, 1).setInt(2, 25).setString(3, "Dave Jones", "UTF-8");

		a2Id = new Tuple(2, ByteOrder.LITTLE_ENDIAN).setInt(0, 1).setInt(1, 1);
	}

	public void testCompositePK() {
		assertEquals(ONE, connection.insertOrReplace(COMPOSITE_SPACE, a2));
		assertEquals(ONE, connection.insertOrReplace(COMPOSITE_SPACE, b2));
		assertEquals(1, connection.find(COMPOSITE_SPACE, 0, 0, 100, a2Id).size());
	}

	public void testBase() {
		Tuple tmp;
		tmp = connection.insertOrReplaceAndGet(PRIM_AND_SEC_SPACE, a);
		assertTrue(Arrays.equals(tmp.pack(), a.pack()));

		assertEquals(ONE, connection.insertOrReplace(PRIM_AND_SEC_SPACE, b));
		assertEquals(ONE, connection.insertOrReplace(PRIM_AND_SEC_SPACE, c));
		assertEquals(ONE, connection.insertOrReplace(PRIM_AND_SEC_SPACE, d));

		assertEquals(ONE, connection.replace(PRIM_AND_SEC_SPACE, a));

		try {
			assertEquals(ONE, connection.insert(PRIM_AND_SEC_SPACE, a));
			fail();
		} catch (TarantoolException ignored) {

		}

		connection.findOne(PRIM_AND_SEC_SPACE, 0, 0, 1, Arrays.asList(aId));
		assertTrue(Arrays.equals(tmp.pack(), a.pack()));

		tmp = connection.find(PRIM_AND_SEC_SPACE, 0, 0, 1, Arrays.asList(aId)).get(0);

		assertTrue(Arrays.equals(tmp.pack(), a.pack()));

		assertEquals(1, connection.find(PRIM_AND_SEC_SPACE, 0, 0, 1, Arrays.asList(aId)).size());

		assertNull(connection.findOne(PRIM_AND_SEC_SPACE, 0, 0, 1, Arrays.asList(eId)));

		assertEquals(2, connection.find(PRIM_AND_SEC_SPACE, 1, 0, 2, Arrays.asList(secKeyA)).size());

		assertEquals(3, connection.find(PRIM_AND_SEC_SPACE, 1, 0, 3, Arrays.asList(secKeyA, secKeyB)).size());

		assertTrue(Arrays.equals(connection.deleteAndGet(PRIM_AND_SEC_SPACE, dId).pack(), d.pack()));

		assertEquals(2, connection.updateAndGet(PRIM_AND_SEC_SPACE, aId, Arrays.asList(new Operation(UP.ADD, 2, incrementAccessA))).getInt(2));

		assertEquals("Johny Smith",
				connection.updateAndGet(PRIM_AND_SEC_SPACE, aId, Arrays.asList(new Operation(UP.SPLICE, 3, updateNameA))).getString(3, "UTF-8"));

		assertEquals("[FIRED]", connection.updateAndGet(PRIM_AND_SEC_SPACE, aId, Arrays.asList(new Operation(UP.INSERT, 3, addMarkA))).getString(3, "UTF-8"));

		assertEquals("Johny Smith", connection.updateAndGet(PRIM_AND_SEC_SPACE, aId, Arrays.asList(new Operation(UP.DELETE, 3, empty))).getString(3, "UTF-8"));

		assertEquals("John Smith", connection.updateAndGet(PRIM_AND_SEC_SPACE, aId, Arrays.asList(new Operation(UP.SET, 3, returnNameA))).getString(3, "UTF-8"));

	}
}
