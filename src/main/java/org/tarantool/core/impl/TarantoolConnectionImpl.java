package org.tarantool.core.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.tarantool.core.Operation;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.Tuple;
import org.tarantool.core.cmd.Delete;
import org.tarantool.core.cmd.Insert;
import org.tarantool.core.cmd.Ping;
import org.tarantool.core.cmd.Response;
import org.tarantool.core.cmd.Select;
import org.tarantool.core.cmd.Transport;
import org.tarantool.core.cmd.Update;
import org.tarantool.core.proto.Flags;
import org.tarantool.pool.ConnectionReturnPoint;
import org.tarantool.pool.Returnable;

/**
 * <p>
 * TarantoolConnectionImpl class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class TarantoolConnectionImpl implements TarantoolConnection, Returnable {
	Transport transport;
	ConnectionReturnPoint returnPoint;
	AtomicInteger id = new AtomicInteger();

	/**
	 * <p>
	 * Constructor for TarantoolConnectionImpl.
	 * </p>
	 * 
	 * @param transport
	 *            a {@link org.tarantool.core.cmd.Transport} object.
	 */
	public TarantoolConnectionImpl(Transport transport) {
		this.transport = transport;
	}

	/** {@inheritDoc} */
	@Override
	public void returnTo(ConnectionReturnPoint returnPoint) {
		this.returnPoint = returnPoint;
	}

	/** {@inheritDoc} */
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

	/** {@inheritDoc} */
	@Override
	public Tuple deleteAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Delete(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}
	}

	/** {@inheritDoc} */
	@Override
	public Tuple updateAndGet(int space, Tuple tuple, List<Operation> ops) {
		try {
			Response response = transport.execute(new Update(id.incrementAndGet(), tuple, ops).space(space).flags(Flags.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Integer update(int space, Tuple tuple, List<Operation> ops) {
		try {
			Response response = transport.execute(new Update(id.incrementAndGet(), tuple, ops).space(space));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Tuple insertAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.RETURN_TUPLE | Flags.ADD_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Integer insert(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.ADD_TUPLE));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Integer replace(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.REPLACE_TUPLE));
			return response.getCount();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Tuple replaceAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.REPLACE_TUPLE | Flags.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Tuple insertOrReplaceAndGet(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space).flags(Flags.RETURN_TUPLE));
			return response.readSingleTuple();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Integer insertOrReplace(int space, Tuple tuple) {
		try {
			Response response = transport.execute(new Insert(id.incrementAndGet(), tuple.pack()).space(space));
			return response.getCount();
		} finally {
			returnConnection();
		}
	}

	/** {@inheritDoc} */
	@Override
	public List<Tuple> find(int space, int index, int offset, int limit, Tuple... keys) {
		try {
			Response response = transport.execute(new Select(id.incrementAndGet(), keys).space(space).index(index).offset(offset).limit(limit));
			return response.readTuples();
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public List<Tuple> find(int space, int index, int offset, int limit, Collection<Tuple> keys) {
		return find(space, index, offset, limit, keys.toArray(new Tuple[keys.size()]));
	}

	/** {@inheritDoc} */
	@Override
	public Tuple findOne(int space, int index, int offset, Tuple... keys) {
		try {
			Response response = transport.execute(new Select(id.incrementAndGet(), keys).space(space).index(index).offset(offset).limit(1));
			List<Tuple> tuples = response.readTuples();
			return tuples == null || tuples.isEmpty() ? null : tuples.get(0);
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public Tuple findOne(int space, int index, int offset, Collection<Tuple> keys) {
		return findOne(space, index, offset, keys.toArray(new Tuple[keys.size()]));
	}

	/** {@inheritDoc} */
	@Override
	public Boolean ping() {
		try {
			transport.execute(new Ping(id.get()));
			return true;
		} finally {
			returnConnection();
		}

	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		try {
			transport.close();
		} catch (IOException ignored) {

		}
	}

}
