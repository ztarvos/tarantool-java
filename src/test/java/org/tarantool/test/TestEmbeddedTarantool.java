package org.tarantool.test;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.tarantool.core.Const.UP;
import org.tarantool.core.Operation;
import org.tarantool.core.TarantoolClient;
import org.tarantool.core.Tuple;
import org.tarantool.core.impl.TarantoolClientImpl;

public class TestEmbeddedTarantool {
	@Test
	public void testCycle() {
		InMemoryTarantoolImpl testTarantool = new InMemoryTarantoolImpl();
		testTarantool.initSpace(0);
		testTarantool.initSecondaryKey(0, 1, 1, 2);
		TarantoolClient client = new TarantoolClientImpl(testTarantool);

		String msg = "Hello world!";
		Tuple testTuple = new Tuple(4, LITTLE_ENDIAN).setInt(0, 1).setString(1, msg, "UTF-8").setLong(2, 188L).setDouble(3, 0.55);
		Tuple testTuple2 = new Tuple(4, LITTLE_ENDIAN).setInt(0, 2).setString(1, msg, "UTF-8").setLong(2, 188L).setDouble(3, 0.88);
		Tuple id = new Tuple(1, LITTLE_ENDIAN).setInt(0, 1);
		Tuple id2 = new Tuple(1, LITTLE_ENDIAN).setInt(0, 2);
		Tuple stored = client.insertOrReplaceAndGet(0, testTuple);
		Tuple secKey = new Tuple(2, LITTLE_ENDIAN).setString(0, msg, "UTF-8").setLong(1, 188);

		assertTrue(Arrays.equals(stored.pack(), testTuple.pack()));

		assertNotNull(client.findOne(0, 0, 0, 1, id));

		assertNotNull(client.findOne(0, 1, 0, 1, secKey));

		assertNotNull(client.insertOrReplaceAndGet(0, testTuple2));
		assertEquals(2, client.find(0, 1, 0, 2, secKey).size());

		assertTrue(Arrays.equals(testTuple2.pack(), client.deleteAndGet(0, id2).pack()));
		assertNull(client.findOne(0, 0, 0, 1, id2));
		assertEquals(1, client.find(0, 1, 0, 2, secKey).size());
		assertEquals(194L, client.updateAndGet(0, id, asList(new Operation(UP.ADD, 2, new Tuple(1, LITTLE_ENDIAN).setInt(0, 6)))).getLong(2));
		assertEquals(20120810, client.updateAndGet(0, id, asList(new Operation(UP.INSERT, 4, new Tuple(1, LITTLE_ENDIAN).setInt(0, 20120810)))).getInt(4));
		assertEquals(79991234567L, client.updateAndGet(0, id, asList(new Operation(UP.INSERT, 4, new Tuple(1, LITTLE_ENDIAN).setLong(0, 79991234567L))))
				.getLong(4));
		assertEquals(5, client.updateAndGet(0, id, asList(new Operation(UP.DELETE, 4, new Tuple(1, LITTLE_ENDIAN).setInt(0, 0)))).size());
		assertEquals(
				"Aloha world!",
				client.updateAndGet(0, id,
						asList(new Operation(UP.SPLICE, 1, new Tuple(3, LITTLE_ENDIAN).setInt(0, 0).setInt(1, 5).setString(2, "Aloha", "UTF-8")))).getString(1,
						"UTF-8"));

	}
}
