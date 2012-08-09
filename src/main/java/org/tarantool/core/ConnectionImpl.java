package org.tarantool.core;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.tarantool.core.cmd.Delete;
import org.tarantool.core.cmd.Insert;
import org.tarantool.core.cmd.Ping;
import org.tarantool.core.cmd.Select;
import org.tarantool.core.cmd.Update;

public class ConnectionImpl implements Connection,Returnable {
	Transport transport;
	ConnectionReturnPoint returnPoint;
	AtomicInteger id = new AtomicInteger();

	public ConnectionImpl(Transport transport) {
		this.transport = transport;
	}

	@Override
	public void returnTo(ConnectionReturnPoint returnPoint) {
		this.returnPoint = returnPoint;
	}

	@Override
	public Integer delete(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Delete(id.incrementAndGet(), tuple.pack()).space(space));
			return response.getCount();
		} finally {
			returnConnection();
		}
	}

	private void returnConnection() {
		if (returnPoint != null) {
			returnPoint.returnConnection(this);
			returnPoint = null;
		}
	}

	@Override
	public Tuple deleteAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Delete(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}
	}

	@Override
	public Tuple updateAndGet(int space, Tuple tuple, List<Operation> ops) {
		try {
			Response response = transport.execute(new Update(id.incrementAndGet(), tuple, ops).space(space).flags(Const.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Integer update(int space, Tuple tuple, List<Operation> ops) {
		try {
			Response response = transport.execute(new Update(id.incrementAndGet(), tuple, ops).space(space));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Tuple insertAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.RETURN_TUPLE | Const.ADD_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Integer insert(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.ADD_TUPLE));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Integer replace(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.REPLACE_TUPLE));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Tuple replaceAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.REPLACE_TUPLE|Const.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Tuple insertOrReplaceAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Const.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	@Override
	public Integer insertOrReplace(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space));
			return response.getCount();
		} finally {
			returnConnection();
		}
	}

	@Override
	public List<Tuple> find(int space, int index, int offset, int limit, Tuple... keys) {
		try {
			Response response = transport.execute(new Select(id.incrementAndGet(), keys).space(space).index(index).offset(offset).limit(limit));
			return response.readTuples();
		} finally {
			returnConnection();
		}

	}

	@Override
	public List<Tuple> find(int space, int index, int offset, int limit, Collection<Tuple> keys) {
		return find(space, index, offset, limit, keys.toArray(new Tuple[keys.size()]));
	}

	@Override
	public Tuple findOne(int space, int index, int offset, int limit, Tuple... keys) {
		try {
			Response response = transport.execute(new Select(id.incrementAndGet(), keys).space(space).index(index).offset(offset).limit(limit));
			List<Tuple> tuples = response.readTuples();
			return tuples==null||tuples.isEmpty()?null:tuples.get(0);
		} finally {
			returnConnection();
		}

	}

	@Override
	public Tuple findOne(int space, int index, int offset, int limit, Collection<Tuple> keys) {
		return findOne(space, index, offset, limit, keys.toArray(new Tuple[keys.size()]));
	}

	@Override
	public Boolean ping() {
		try {
			transport.execute(new Ping(id.get()));
			return true;
		} finally {
			returnConnection();
		}

	}

	@Override
	public void close() {
		try {
			transport.close();
		} catch (IOException ignored) {

		}
	}

}