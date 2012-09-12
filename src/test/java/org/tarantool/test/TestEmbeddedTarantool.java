package org.tarantool.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.tarantool.core.StandardTest;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.Tuple;
import org.tarantool.core.impl.TarantoolConnectionImpl;
import org.tarantool.test.InMemoryTarantoolImpl.CallStub;

public class TestEmbeddedTarantool {

	@Test
	public void standardTest() {
		InMemoryTarantoolImpl test = new InMemoryTarantoolImpl();
		TarantoolConnection connection = new TarantoolConnectionImpl(test);
		StandardTest st = new StandardTest(connection);
		test.initSpace(StandardTest.PRIM_AND_SEC_SPACE, 0);
		test.initSecondaryKey(StandardTest.PRIM_AND_SEC_SPACE, 1, false, 1, 2);
		test.initSpace(StandardTest.COMPOSITE_SPACE, 0, 1);
		test.initSpace(StandardTest.CALL_SPACE, 0, 0);
		test.initProc("box.delete", new CallStub() {

			@Override
			public List<Tuple> call(InMemoryTarantoolImpl impl, String procName, int flags, Tuple args) {
				Tuple pk = impl.copyExcept(args, 0);
				int space = Integer.parseInt(args.getString(0, "UTF-8"));
				Tuple deleted = impl.delete(space, pk);
				return deleted == null ? new ArrayList<Tuple>() : Arrays.asList(deleted);
			}
		});
		test.initProc("box.insert", new CallStub() {

			@Override
			public List<Tuple> call(InMemoryTarantoolImpl impl, String procName, int flags, Tuple args) {
				Tuple tuple = impl.copyExcept(args, 0);
				int space = Integer.parseInt(args.getString(0, "UTF-8"));
				Tuple put = impl.put(space, tuple, true, false);
				return Arrays.asList(put);
			}
		});
		test.initProc("box.select_range", new CallStub() {

			@Override
			public List<Tuple> call(InMemoryTarantoolImpl impl, String procName, int flags, Tuple args) {
				int spaceNo = Integer.parseInt(args.getString(0, "UTF-8"));
				int indexNo = Integer.parseInt(args.getString(1, "UTF-8"));
				int limit = Integer.parseInt(args.getString(2, "UTF-8"));
				if (args.size() == 3) {
					return impl.shiftAndLimit(0, limit, impl.all(spaceNo, indexNo));
				} else {
					Tuple key = impl.copyExcept(args, 0, 1, 2);
					List<Tuple> head = impl.head(spaceNo, indexNo, key);
					List<Tuple> value = impl.get(spaceNo, indexNo, key);
					if (value != null && value.size() > 0) {
						head.addAll(0, value);
					}
					return impl.shiftAndLimit(0, limit, head);
				}
			}
		});
		st.run();
		connection.close();
	}
}
