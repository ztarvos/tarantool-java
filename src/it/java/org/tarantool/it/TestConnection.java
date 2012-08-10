package org.tarantool.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.Test;
import org.tarantool.core.Const.UP;
import org.tarantool.core.Operation;
import org.tarantool.core.TarantoolClient;
import org.tarantool.core.Tuple;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.SocketChannelTarantoolClient;

public class TestConnection {

	@Test
	public void testCycle() {
		TarantoolClient connection = new SocketChannelTarantoolClient();
		// InMemoryTarantoolImpl transport = new InMemoryTarantoolImpl();
		// transport.initSpace(0);
		// TarantoolClient connection = new TarantoolClientImpl(transport);
		Tuple tuple = new Tuple(3, ByteOrder.LITTLE_ENDIAN);
		tuple.setInt(0, 211);
		tuple.setInt(1, 123);
		tuple.setString(2, "Hello world!", "UTF-8");
		Tuple inserted = connection.insertOrReplaceAndGet(0, tuple);
		assertNotNull(inserted);
		tuple.setInt(1, 231);
		assertNotNull(connection.replaceAndGet(0, tuple));

		try {
			connection.insert(0, tuple);
			fail();
		} catch (TarantoolException ignored) {

		}
		Tuple id = new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 211);
		assertNotNull(connection.findOne(0, 0, 0, 1, id));
		Tuple updated = connection.updateAndGet(0, id, Arrays.asList(new Operation(UP.ADD, 1, new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 10))));
		assertEquals(241, updated.getInt(1));

		updated = connection.updateAndGet(0, id, Arrays.asList(new Operation(UP.INSERT, 3, new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 10))));
		assertEquals(10, updated.getInt(3));
		assertEquals(4, updated.size());
		updated = connection.updateAndGet(0, id, Arrays.asList(new Operation(UP.DELETE, 3, new Tuple(1, ByteOrder.LITTLE_ENDIAN).setInt(0, 0))));
		assertEquals(3, updated.size());
		assertEquals(
				"Aloha world!",
				connection.updateAndGet(
						0,
						id,
						Arrays.asList(new Operation(UP.SPLICE, 2, new Tuple(3, ByteOrder.LITTLE_ENDIAN).setInt(0, 0).setInt(1, 5)
								.setString(2, "Aloha", "UTF-8")))).getString(2, "UTF-8"));
		assertNotNull(connection.deleteAndGet(0, id));
		assertNull(connection.deleteAndGet(0, id));

		connection.close();
	}
}
