package org.tarantool.test;

import org.junit.Test;
import org.tarantool.core.StandardTest;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.impl.TarantoolConnectionImpl;

public class TestEmbeddedTarantool {

	@Test
	public void standardTest() {
		InMemoryTarantoolImpl test = new InMemoryTarantoolImpl();
		TarantoolConnection connection = new TarantoolConnectionImpl(test);
		StandardTest st = new StandardTest(connection);
		test.initSpace(StandardTest.PRIM_AND_SEC_SPACE, 0);
		test.initSecondaryKey(StandardTest.PRIM_AND_SEC_SPACE, 1, false, 1, 2);
		test.initSpace(StandardTest.COMPOSITE_SPACE, 0, 1);
		st.run();
		connection.close();
	}

}
