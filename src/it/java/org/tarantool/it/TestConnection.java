package org.tarantool.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.Test;
import org.tarantool.core.Connection;
import org.tarantool.core.Const.UP;
import org.tarantool.core.Operation;
import org.tarantool.core.SocketChannelConnectionFactory;
import org.tarantool.core.Tuple;
import org.tarantool.core.exception.TarantoolException;

public class TestConnection {
	

	@Test
	public void testCycle() {
		SocketChannelConnectionFactory factory = new SocketChannelConnectionFactory();
		Connection connection = factory.getConnection();
		Tuple tuple = new Tuple(2, ByteOrder.LITTLE_ENDIAN);
		tuple.setInt(0, 211);
		tuple.setInt(1, 123);
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
		Tuple updated = connection.updateAndGet(0, id, Arrays.asList(new Operation(UP.ADD, 1, new Tuple(1,ByteOrder.LITTLE_ENDIAN).setInt(0,10))));
		assertEquals(241,updated.getInt(1));
		assertNotNull(connection.deleteAndGet(0, id));
		connection.close();
	}
}
