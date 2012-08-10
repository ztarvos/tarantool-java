package org.tarantool.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.tarantool.core.Const;
import org.tarantool.core.Const.OP;
import org.tarantool.core.Const.UP;
import org.tarantool.core.Request;
import org.tarantool.core.Response;
import org.tarantool.core.SingleQueryClientFactory;
import org.tarantool.core.TarantoolClient;
import org.tarantool.core.Transport;
import org.tarantool.core.Tuple;
import org.tarantool.core.cmd.DMLRequest;
import org.tarantool.core.cmd.Select;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.TarantoolClientImpl;

public class InMemoryTarantoolImpl implements SingleQueryClientFactory, Transport {

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
		ConcurrentMap<Integer, SecondaryKey> secondaryKeys = new ConcurrentHashMap<Integer, InMemoryTarantoolImpl.SecondaryKey>();
	}

	ConcurrentMap<Integer, Space> spaces = new ConcurrentHashMap<Integer, Space>();

	public void initSpace(int num) {
		spaces.put(num, new Space());
	}

	public void initSecondaryKey(int spaceNum, int keyNum, int... fields) {
		Space space = spaces.get(spaceNum);
		space.secondaryKeys.putIfAbsent(keyNum, new SecondaryKey(fields));
	}

	protected Tuple put(int spaceNum, Tuple tuple, boolean insert, boolean replace) {
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
				BigInteger secondaryKey = toBigInteger(tuple, key.fields);
				List<Tuple> collection = key.idx.get(secondaryKey);
				if (collection == null) {
					key.idx.put(secondaryKey, collection = new ArrayList<Tuple>());
				}
				collection.add(tuple);
			}
		}
		return tuple;

	}

	protected BigInteger toBigInteger(Tuple tuple, int... fields) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			os.write(1);
			for (int f : fields) {
				os.write(tuple.getBytes(f));
			}
			os.write(1);
			os.close();
		} catch (IOException ignored) {

		}
		BigInteger theKey = new BigInteger(os.toByteArray());
		return theKey;
	}

	protected BigInteger toBigInteger(Tuple tuple) {
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			os.write(1);
			for (int i = 0; i < tuple.size(); i++) {

				os.write(tuple.getBytes(i));

			}
			os.write(1);
			os.close();
		} catch (IOException ignored) {

		}
		BigInteger theKey = new BigInteger(os.toByteArray());
		return theKey;
	}

	protected List<Tuple> get(int spaceNum, int idx, Tuple t) {
		Space space = spaces.get(spaceNum);
		if (idx == 0) {
			if (t.size() != 1) {
				throw new TarantoolException(47, String.format("Key part count %d is greater than index part count %d", t.size(), 1));
			}
			Tuple tuple = space.primaryKey.get(toBigInteger(t, 0));
			return tuple == null ? new ArrayList<Tuple>() : new ArrayList<Tuple>(Arrays.asList(tuple));
		} else {
			SecondaryKey secKey = space.secondaryKeys.get(idx);
			if (secKey == null) {
				throw new TarantoolException(53, String.format("No index #%u is defined in space %u", idx, spaceNum));
			}
			if (t.size() != secKey.fields.length) {
				throw new TarantoolException(47, String.format("Key part count %d is greater than index part count %d", t.size(), 1));
			}
			List<Tuple> result = secKey.idx.get(toBigInteger(t));
			return result == null ? new ArrayList<Tuple>() : result;
		}
	}

	protected Tuple delete(int spaceNum, Tuple t) {
		Space space = spaces.get(spaceNum);
		BigInteger pk = toBigInteger(t, 0);
		Tuple stored = space.primaryKey.get(pk);
		if (stored != null && !space.secondaryKeys.isEmpty()) {
			for (SecondaryKey key : space.secondaryKeys.values()) {
				BigInteger theKey = toBigInteger(stored, key.fields);
				Collection<Tuple> collection = key.idx.get(theKey);
				if (collection != null) {
					collection.remove(stored);
				}

			}
		}
		space.primaryKey.remove(pk);
		return stored;
	}

	protected List<Tuple> shiftAndLimit(int offset, int limit, List<Tuple> result) {
		for (int i = 0; i < offset && !result.isEmpty(); i++)
			result.remove(0);
		while (result.size() > limit)
			result.remove(result.size() - 1);
		return result;
	}

	@Override
	public TarantoolClient getSingleQueryConnection() {
		return new TarantoolClientImpl(this);
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public synchronized Response execute(Request request) {
		OP op = request.getOp();
		if (op == OP.PING) {
			return new Response(OP.PING.type, 0, request.getId());
		} else if (op == OP.UPDATE || op == OP.INSERT || op == OP.DELETE) {
			return executeDML(request, op);

		} else if (op == OP.SELECT) {
			return executeSelect(request);
		}
		throw new TarantoolException(2, String.format("Illegal parameters, %s", "Unknown operation " + op));
	}

	private Response executeSelect(Request request) {
		Select select = ((Select) request);
		List<Tuple> result = new ArrayList<Tuple>();
		for (int i = 0; i < select.getBody().length; i++) {
			Tuple key = Tuple.create(ByteBuffer.wrap(select.getBody()[i]).order(ByteOrder.LITTLE_ENDIAN), ByteOrder.LITTLE_ENDIAN);
			result.addAll(get(select.getSpace(), select.getIndex(), key));
		}
		shiftAndLimit(select.getOffset(), select.getLimit(), result);
		byte[][] responseBody = new byte[result.size()][];
		int len = 4;
		for (int i = 0; i < result.size(); i++) {
			responseBody[i] = result.get(i).pack();
			len += responseBody[i].length + 4;
		}
		Response response = new Response(OP.SELECT.type, len, request.getId());
		ByteBuffer bodyBuffer = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN).putInt(result.size());
		for (byte[] tuple : responseBody) {
			bodyBuffer.putInt(tuple.length).put(tuple);
		}
		response.setBody(bodyBuffer.array());
		return response;
	}

	private Response executeDML(Request request, OP op) {
		DMLRequest<?> dmlRequest = (DMLRequest<?>) request;
		int spaceNum = dmlRequest.space();
		int flags = dmlRequest.flags();
		byte[] body = dmlRequest.getBody();
		ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
		Tuple pk = Tuple.create(buffer, ByteOrder.LITTLE_ENDIAN);
		List<Tuple> values = get(spaceNum, 0, new Tuple(1, ByteOrder.LITTLE_ENDIAN).setBytes(0, pk.getBytes(0)));
		Tuple stored = values == null || values.isEmpty() ? null : values.get(0);
		if (stored == null && op != OP.INSERT) {
			Response response = new Response(op.type, 4, request.getId());
			response.setBody(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array());
			return response;
		}
		if (op == OP.INSERT) {
			stored = put(spaceNum, pk, (flags & Const.ADD_TUPLE) > 0, (flags & Const.REPLACE_TUPLE) > 0);
		} else if (op == OP.DELETE) {
			stored = delete(spaceNum, pk);
		} else if (op == OP.UPDATE) {
			int ops = buffer.getInt();
			for (int i = 0; i < ops; i++) {
				update(spaceNum, buffer, pk);
			}
			stored = get(spaceNum, 0, new Tuple(1, ByteOrder.LITTLE_ENDIAN).setBytes(0, pk.getBytes(0))).get(0);
		}

		if ((dmlRequest.getFlags() | Const.RETURN_TUPLE) > 0) {
			byte[] responseBody = stored.pack();
			Response response = new Response(op.type, responseBody.length + 8, request.getId());
			response.setBody(ByteBuffer.allocate(responseBody.length + 8).order(ByteOrder.LITTLE_ENDIAN).putInt(1).putInt(responseBody.length)
					.put(responseBody).array());
			return response;
		} else {
			Response response = new Response(op.type, 4, request.getId());
			response.setBody(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(1).array());
			return response;
		}
	}

	private void update(int spaceNum, ByteBuffer buffer, Tuple tuple) {
		int fieldNo = buffer.getInt();
		UP up = UP.valueOf((int) buffer.get());

		Tuple args = null;
		if (up.args > 0) {
			args = Tuple.createFromPackedFields(buffer, ByteOrder.LITTLE_ENDIAN, 1);
		}
		if (up.args > 1) {
			args = Tuple.createFromPackedFields(ByteBuffer.wrap(args.getBytes(0)), ByteOrder.LITTLE_ENDIAN, up.args);
		}
		Space space = spaces.get(spaceNum);
		BigInteger pk = toBigInteger(tuple, 0);
		Tuple stored = space.primaryKey.get(pk);
		if (stored != null) {
			if (stored.size() < fieldNo || fieldNo < 0) {
				throw new TarantoolException(54, String.format("Field %d was not found in the tuple", fieldNo));
			}
			if (up == UP.ADD || up == UP.AND || up == UP.XOR || up == UP.OR) {
				int storedFieldLength = stored.getBytes(fieldNo).length;
				if (storedFieldLength == 4) {
					stored.setInt(fieldNo, (int) arithmeticUpdate(up, stored.getInt(fieldNo), args.getInt(0)));
				} else if (storedFieldLength == 8) {
					stored.setLong(fieldNo, arithmeticUpdate(up, stored.getLong(fieldNo), args.getBytes(0).length == 4 ? args.getInt(0) : args.getLong(0)));
				} else {
					throw new TarantoolException(40, String.format("Field type does not match one required by operation: expected a %s", "NUM or NUM 64"));
				}

			} else if (up == UP.DELETE) {
				stored = deleteField(fieldNo, stored);
				if (stored.size() < 2) {
					throw new TarantoolException(25, "UPDATE error: the new tuple has no fields");
				}
			} else if (up == UP.INSERT) {
				stored = insertField(fieldNo, args, stored);
			} else if (up == UP.SPLICE) {
				splice(fieldNo, args, stored);
			}
			delete(spaceNum, tuple);
			put(spaceNum, stored, true, false);
		}
	}

	protected void splice(int fieldNo, Tuple args, Tuple stored) {
		byte[] fieldValue = stored.getBytes(fieldNo);
		int from = args.getInt(0);
		int len = args.getInt(1);
		byte[] insert = args.getBytes(2);
		ByteBuffer resultBuf = ByteBuffer.allocate(fieldValue.length - len + insert.length).order(ByteOrder.LITTLE_ENDIAN);
		stored.setBytes(fieldNo,
				resultBuf.put(Arrays.copyOfRange(fieldValue, 0, from)).put(insert).put(Arrays.copyOfRange(fieldValue, from + len, fieldValue.length)).array());
	}

	protected Tuple insertField(int fieldNo, Tuple args, Tuple stored) {
		Tuple copy = new Tuple(stored.size() + 1, ByteOrder.LITTLE_ENDIAN);
		for (int i = 0, offset = 0; i < stored.size() + 1; i++) {
			if (i != fieldNo) {
				copy.setBytes(i, stored.getBytes(i - offset));
			} else {
				copy.setBytes(i, args.getBytes(0));
				offset = 1;
			}
		}
		return copy;
	}

	protected Tuple deleteField(int fieldNo, Tuple stored) {
		Tuple copy = new Tuple(stored.size() - 1, ByteOrder.LITTLE_ENDIAN);
		for (int i = 0, offset = 0; i < copy.size(); i++) {
			if (i == fieldNo) {
				offset = 1;
			}
			copy.setBytes(i, stored.getBytes(i + offset));
		}
		return copy;
	}

	protected long arithmeticUpdate(UP up, long value, long arg) {
		if (up == UP.ADD)
			value += arg;
		else if (up == UP.AND)
			value &= arg;
		else if (up == UP.XOR)
			value ^= arg;
		else if (up == UP.OR)
			value |= arg;
		return value;
	}

}
