package org.tarantool.it;

import org.junit.Test;
import org.tarantool.core.StandardTest;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.impl.SocketChannelTarantoolConnection;

public class TestClient {

	@Test
	public void standardTest() {

		TarantoolConnection connection = new SocketChannelTarantoolConnection("localhost", 33313);

		StandardTest st = new StandardTest(connection);
		st.run();

		connection.close();
	}

}
