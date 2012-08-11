package org.tarantool.it;

import org.junit.Test;
import org.tarantool.core.StandardTest;
import org.tarantool.core.TarantoolClient;
import org.tarantool.core.impl.SocketChannelTarantoolClient;

public class TestClient {

	@Test
	public void standardTest() {

		TarantoolClient connection = new SocketChannelTarantoolClient("localhost", 33313);

		StandardTest st = new StandardTest(connection);
		st.run();

		connection.close();
	}

}
