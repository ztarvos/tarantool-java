package org.tarantool.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.tarantool.core.Connection;
import org.tarantool.core.ConnectionImpl;
import org.tarantool.core.Const;
import org.tarantool.core.Const.UP;
import org.tarantool.core.Request;
import org.tarantool.core.Response;
import org.tarantool.core.SingleQueryConnectionFactory;
import org.tarantool.core.Transport;
import org.tarantool.core.Tuple;
import org.tarantool.core.Const.OP;
import org.tarantool.core.cmd.DMLRequest;
import org.tarantool.core.cmd.Insert;
import org.tarantool.core.exception.TarantoolException;

public class TestTarantool implements SingleQueryConnectionFactory, Transport {

	class SecondaryKey {
		int[] fields;
		ConcurrentMap<BigInteger, List<Tuple>> idx = new ConcurrentHashMap<BigInteger, List<Tuple>>();

		private SecondaryKey(int... fields) {
			super();
			this.fields = fields;
		}

	}

	class Space {
		ConcurrentMap<BigInteger, Tuple> primaryKey = new ConcurrentHashMap<BigInteger, Tuple>();
		ConcurrentMap<Integer, SecondaryKey> secondaryKeys = new ConcurrentHashMap<Integer, TestTarantool.SecondaryKey>();
	}

	ConcurrentMap<Integer, Space> spaces = new ConcurrentHashMap<Integer, Space>();

	public void initSpace(int num) {
		spaces.put(num, new Space());
	}

	public void initSecondaryKey(int spaceNum, int keyNum, int... fields) {
		Space space = spaces.get(spaceNum);
		space.secondaryKeys.putIfAbsent(keyNum, new SecondaryKey(fields));
	}

	Tuple put(int spaceNum, Tuple tuple, boolean insert, boolean replace) {
		Space space = spaces.get(spaceNum);
		BigInteger pKey = toBigInteger(tuple, 0);

		if (space.primaryKey.containsKey(pKey)) {
			if (insert) {
				throw new TarantoolException(55, "Tuple already exists");
			}
			delete(spaceNum, tuple);
		} else if (replace) {
			throw new TarantoolException(49, "Tuple doesn't exist");
		}
		space.primaryKey.put(pKey, tuple);
		if (!space.secondaryKeys.isEmpty()) {
			for (SecondaryKey key : space.secondaryKeys.values()) {
				List<Tuple> collection = key.idx.get(toBigInteger(tuple, key.fields));
				if (collection == null) {
					key.idx.put(pKey, collection = new ArrayList<Tuple>());
				}
				collection.add(tuple);
			}
		}
		return tuple;

	}

	private BigInteger toBigInteger(Tuple tuple, int... fields) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		for (int f : fields) {
			try {
				os.write(tuple.getBytes(f));
			} catch (IOException ignored) {

			}
		}
		BigInteger theKey = new BigInteger(os.toByteArray());
		return theKey;
	}

	private BigInteger toBigInteger(Tuple tuple) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		for (int i = 0; i < tuple.size(); i++) {
			try {
				os.write(tuple.getBytes(i));
			} catch (IOException ignored) {

			}
		}
		BigInteger theKey = new BigInteger(os.toByteArray());
		return theKey;
	}

	List<Tuple> get(int spaceNum, int idx, Tuple t) {
		Space space = spaces.get(spaceNum);
		if (idx == 0) {
			if (t.size() != 1) {
				throw new TarantoolException(47, String.format("Key part count %d is greater than index part count %d", t.size(), 1));
			}
			return new ArrayList<Tuple>(Arrays.asList(space.primaryKey.get(toBigInteger(t, 0))));
		} else {
			SecondaryKey secKey = space.secondaryKeys.get(idx);
			if (secKey == null) {
				throw new TarantoolException(53, String.format("No index #%u is defined in space %u", idx, spaceNum));
			}
			if (t.size() != secKey.fields.length) {
				throw new TarantoolException(47, String.format("Key part count %d is greater than index part count %d", t.size(), 1));
			}
			return secKey.idx.get(toBigInteger(t));
		}
	}

	Tuple delete(int spaceNum, Tuple t) {
		Space space = spaces.get(spaceNum);
		BigInteger pk = toBigInteger(t, 0);

		Tuple stored = space.primaryKey.get(pk);
		if (stored != null && !space.secondaryKeys.isEmpty()) {
			for (SecondaryKey key : space.secondaryKeys.values()) {
				BigInteger theKey = toBigInteger(t, key.fields);
				Collection<Tuple> collection = key.idx.get(theKey);
				if (collection != null) {
					collection.remove(t);
				}

			}
		}
		return stored;
	}

	List<Tuple> findBySecKey(int space, int index, Tuple... tuple) {
		return null;
	}

	@Override
	public Connection getSingleQueryConnection() {
		return new ConnectionImpl(this);
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public Response execute(Request request) {
		OP op = request.getOp();
		if (op == OP.PING) {
			return new Response(OP.PING.type, 0, request.getId());
		} else if (op == OP.UPDATE || op == OP.INSERT || op == OP.DELETE) {
			DMLRequest<?> dmlRequest = (DMLRequest<?>) request;
			int space = dmlRequest.space();
			int flags = dmlRequest.flags();
			byte[] body = dmlRequest.getBody();
			ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
			Tuple tuple = Tuple.create(buffer, ByteOrder.LITTLE_ENDIAN);
			if (op == OP.INSERT) {
				put(space, tuple, (flags & Const.ADD_TUPLE) > 0, (flags & Const.REPLACE_TUPLE) > 0);
			} else if (op == OP.DELETE) {
				delete(space, tuple);
			} else if (op == OP.UPDATE) {
				int ops = buffer.getInt();
				for (int i = 0; i < ops; i++) {
					int fieldNo = buffer.getInt();
					UP up = UP.valueOf((int) buffer.get());
					if(up.args>1) {
						
					} else {
						
					}
				}

			}
		}
		throw new TarantoolException(2, String.format("Illegal parameters, %s", "Unknown operation " + op));
	}

}
