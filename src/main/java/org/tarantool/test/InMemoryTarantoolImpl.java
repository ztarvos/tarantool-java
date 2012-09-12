package org.tarantool.test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.Tuple;
import org.tarantool.core.cmd.Call;
import org.tarantool.core.cmd.DMLRequest;
import org.tarantool.core.cmd.Delete;
import org.tarantool.core.cmd.Insert;
import org.tarantool.core.cmd.Ping;
import org.tarantool.core.cmd.Request;
import org.tarantool.core.cmd.Response;
import org.tarantool.core.cmd.Select;
import org.tarantool.core.cmd.Transport;
import org.tarantool.core.cmd.Update;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.TarantoolConnectionImpl;
import org.tarantool.core.proto.Flags;
import org.tarantool.core.proto.Updates;
import org.tarantool.pool.SingleQueryConnectionFactory;

/**
 * InMemory implementation of basic Tarantool Box commands
 */
public class InMemoryTarantoolImpl implements SingleQueryConnectionFactory, Transport {
	public interface CallStub {
		List<Tuple> call(InMemoryTarantoolImpl impl, String procName, int flags, Tuple args);
	}

	Map<String, CallStub> calls = new HashMap<String, CallStub>();

	public class Index {
		int[] fields;
		boolean unique;
		private SortedMap<BigInteger, List<Tuple>> idx = new TreeMap<BigInteger, List<Tuple>>();

		private Index(boolean unique, int... fields) {
			super();
			this.unique = unique;
			this.fields = fields;
		}

		public void put(Tuple t) {
			BigInteger key = toKey(copy(t, fields));
			List<Tuple> collection = idx.get(key);
			if (collection == null) {
				idx.put(key, collection = new ArrayList<Tuple>());
			}
			if (unique && collection.size() > 0) {
				throw new TarantoolException(56, "Duplicate key exists in a unique index");
			}
			collection.add(t);
		}

		public List<Tuple> get(Tuple tuple) {
			return idx.get(toKey(tuple));
		}

		public List<Tuple> head(Tuple tuple) {
			return toList(idx.headMap(toKey(tuple)).values());
		}

		protected List<Tuple> toList(Collection<List<Tuple>> values) {
			List<Tuple> res = new ArrayList<Tuple>();
			for (List<Tuple> list : values) {
				res.addAll(list);
			}
			return res;
		}

		public List<Tuple> tail(Tuple tuple) {
			return toList(idx.tailMap(toKey(tuple)).values());
		}

		public Tuple getOne(Tuple tuple) {
			List<Tuple> collection = idx.get(toKey(tuple));
			return collection == null || collection.isEmpty() ? null : collection.get(0);
		}

		public void remove(Tuple stored) {
			BigInteger key = toKey(copy(stored, fields));
			Collection<Tuple> collection = idx.get(key);
			if (collection != null) {
				collection.remove(stored);
				if (collection.isEmpty()) {
					idx.remove(key);
				}
			}

		}

		public List<Tuple> all() {
			return toList(idx.values());
		}

	}

	public class Space {
		Map<Integer, Index> indexes = new TreeMap<Integer, InMemoryTarantoolImpl.Index>();

		Tuple get(Tuple pk) {
			return indexes.get(0).getOne(pk);
		}

		Tuple getByValue(Tuple tuple) {
			return get(toPK(tuple));
		}

		Tuple toPK(Tuple tuple) {
			Index index = indexes.get(0);
			return copy(tuple, index.fields);
		}
	}

	Map<Integer, Space> spaces = new TreeMap<Integer, Space>();

	/**
	 * Creates space with given index and primary key from specified fields
	 * 
	 * @param num
	 * @param pkFields
	 */
	public void initSpace(int num, int... pkFields) {
		Space space = new Space();
		if (pkFields == null || pkFields.length == 0) {
			pkFields = new int[] { 0 };
		}
		space.indexes.put(0, new Index(true, pkFields));
		spaces.put(num, space);
	}

	/**
	 * Initializes secondary index on given space
	 * 
	 * @param spaceNum
	 * @param keyNum
	 * @param unique
	 * @param fields
	 */
	public void initSecondaryKey(int spaceNum, int keyNum, boolean unique, int... fields) {
		Space space = spaces.get(spaceNum);
		space.indexes.put(keyNum, new Index(unique, fields));
	}

	public void initProc(String name, CallStub stub) {
		calls.put(name, stub);
	}

	/**
	 * <p>
	 * put.
	 * </p>
	 * 
	 * @param spaceNum
	 *            a int.
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @param insert
	 *            a boolean.
	 * @param replace
	 *            a boolean.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple put(int spaceNum, Tuple tuple, boolean insert, boolean replace) {
		Space space = spaces.get(spaceNum);

		if (space.getByValue(tuple) != null) {
			if (insert) {
				throw new TarantoolException(55, "Tuple already exists");
			}
			delete(spaceNum, space.toPK(tuple));
		} else if (replace) {
			throw new TarantoolException(49, "Tuple doesn't exist");
		}
		for (Index key : space.indexes.values()) {
			BigInteger secondaryKey = toKey(copy(tuple, key.fields));
			List<Tuple> collection = key.idx.get(secondaryKey);
			if (collection == null) {
				key.idx.put(secondaryKey, collection = new ArrayList<Tuple>());
			}

			collection.add(tuple);
		}

		return tuple;

	}

	/**
	 * <p>
	 * toKey.
	 * </p>
	 * 
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link java.math.BigInteger} object.
	 */
	public BigInteger toKey(Tuple tuple) {
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

	/**
	 * <p>
	 * get.
	 * </p>
	 * 
	 * @param spaceNum
	 *            a int.
	 * @param idx
	 *            a int.
	 * @param t
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link java.util.List} object.
	 */
	public List<Tuple> get(int spaceNum, int idx, Tuple t) {
		Index index = getIndex(spaceNum, idx, t);
		List<Tuple> result = index.idx.get(toKey(t));
		return result == null ? new ArrayList<Tuple>() : result;

	}

	public List<Tuple> head(int spaceNum, int idx, Tuple t) {
		Index index = getIndex(spaceNum, idx, t);
		List<Tuple> result = index.head(t);
		return result == null ? new ArrayList<Tuple>() : result;
	}

	public List<Tuple> all(int spaceNum, int idx) {
		Index index = getIndexWithoutCheck(spaceNum, idx);
		List<Tuple> result = index.all();
		return result == null ? new ArrayList<Tuple>() : result;
	}

	public List<Tuple> tail(int spaceNum, int idx, Tuple t) {
		Index index = getIndex(spaceNum, idx, t);
		List<Tuple> result = index.tail(t);
		return result == null ? new ArrayList<Tuple>() : result;
	}

	public Index getIndex(int spaceNum, int idx, Tuple t) {
		Index index = getIndexWithoutCheck(spaceNum, idx);
		if (t.size() != index.fields.length) {
			throw new TarantoolException(47, String.format("Key part count %d is greater than index part count %d", t.size(), 1));
		}
		return index;
	}

	public Index getIndexWithoutCheck(int spaceNum, int idx) {
		Space space = spaces.get(spaceNum);
		if (space == null) {
			throw new TarantoolException(52, String.format("Space %d is disabled", spaceNum));
		}

		Index index = space.indexes.get(idx);
		if (index == null) {
			throw new TarantoolException(53, String.format("No index #%u is defined in space %u", idx, spaceNum));
		}
		return index;
	}

	/**
	 * <p>
	 * delete.
	 * </p>
	 * 
	 * @param spaceNum
	 *            a int.
	 * @param t
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple delete(int spaceNum, Tuple t) {
		Space space = spaces.get(spaceNum);

		Tuple stored = space.get(t);
		if (stored != null) {
			for (Index key : space.indexes.values()) {
				key.remove(stored);
			}
		}
		return stored;
	}

	/**
	 * <p>
	 * shiftAndLimit.
	 * </p>
	 * 
	 * @param offset
	 *            a int.
	 * @param limit
	 *            a int.
	 * @param result
	 *            a {@link java.util.List} object.
	 * @return a {@link java.util.List} object.
	 */
	public List<Tuple> shiftAndLimit(int offset, int limit, List<Tuple> result) {
		for (int i = 0; i < offset && !result.isEmpty(); i++)
			result.remove(0);
		while (result.size() > limit)
			result.remove(result.size() - 1);
		return result;
	}

	/** {@inheritDoc} */
	@Override
	public TarantoolConnection getSingleQueryConnection() {
		return new TarantoolConnectionImpl(this);
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws IOException {

	}

	/** {@inheritDoc} */
	@Override
	public synchronized Response execute(Request request) {
		int op = request.getOp();
		if (op == Ping.OP_CODE) {
			return new Response(Ping.OP_CODE, 0, request.getId());
		} else if (op == Update.OP_CODE || op == Insert.OP_CODE || op == Delete.OP_CODE) {
			return executeDML(request, op);

		} else if (op == Select.OP_CODE) {
			return executeSelect(request);
		} else if (op == Call.OP_CODE) {
			return executeCall(request);
		}
		throw new TarantoolException(2, String.format("Illegal parameters, %s", "Unknown operation " + op));
	}

	private Response executeCall(Request request) {
		Call call = (Call) request;
		CallStub callStub = calls.get(call.getProcName());
		if (callStub == null) {
			throw new TarantoolException(50, String.format("Procedure '%s' is not defined", call.getProcName()));
		}
		Tuple args = Tuple.create(ByteBuffer.wrap(call.getBody()).order(ByteOrder.LITTLE_ENDIAN), ByteOrder.LITTLE_ENDIAN);
		return packResult(request, callStub.call(this, call.getProcName(), call.getFlags(), args), Call.OP_CODE);
	}

	private Response executeSelect(Request request) {
		Select select = ((Select) request);
		List<Tuple> result = new ArrayList<Tuple>();
		for (int i = 0; i < select.getBody().length; i++) {
			Tuple key = Tuple.create(ByteBuffer.wrap(select.getBody()[i]).order(ByteOrder.LITTLE_ENDIAN), ByteOrder.LITTLE_ENDIAN);
			result.addAll(get(select.getSpace(), select.getIndex(), key));
		}
		shiftAndLimit(select.getOffset(), select.getLimit(), result);
		return packResult(request, result, Select.OP_CODE);
	}

	public Response packResult(Request request, List<Tuple> result, int code) {
		byte[][] responseBody = new byte[result.size()][];
		int len = 4;
		for (int i = 0; i < result.size(); i++) {
			responseBody[i] = result.get(i).pack();
			len += responseBody[i].length + 4;
		}

		Response response = new Response(code, len, request.getId());
		ByteBuffer bodyBuffer = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN).putInt(result.size());
		for (byte[] tuple : responseBody) {
			bodyBuffer.putInt(tuple.length).put(tuple);
		}
		response.setBody(bodyBuffer.array());
		return response;
	}

	private Response executeDML(Request request, int op) {
		DMLRequest<?> dmlRequest = (DMLRequest<?>) request;
		int spaceNum = dmlRequest.space();
		int flags = dmlRequest.flags();
		byte[] body = dmlRequest.getBody();
		ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
		Tuple tuple = Tuple.create(buffer, ByteOrder.LITTLE_ENDIAN);
		Tuple stored = null;
		Space space = spaces.get(spaceNum);
		if (op != Insert.OP_CODE && (stored = space.get(tuple)) == null) {
			Response response = new Response(op, 4, request.getId());
			response.setBody(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0).array());
			return response;
		}

		if (op == Insert.OP_CODE) {
			stored = put(spaceNum, tuple, (flags & Flags.ADD_TUPLE) > 0, (flags & Flags.REPLACE_TUPLE) > 0);
		} else if (op == Delete.OP_CODE) {
			stored = delete(spaceNum, tuple);
		} else if (op == Update.OP_CODE) {
			int ops = buffer.getInt();
			for (int i = 0; i < ops; i++) {
				update(spaceNum, buffer, tuple);
			}
			stored = get(spaceNum, 0, new Tuple(1).setBytes(0, tuple.getBytes(0))).get(0);
		}

		if ((dmlRequest.getFlags() & Flags.RETURN_TUPLE) > 0) {
			byte[] responseBody = stored.pack();
			Response response = new Response(op, responseBody.length + 8, request.getId());
			response.setBody(ByteBuffer.allocate(responseBody.length + 8).order(ByteOrder.LITTLE_ENDIAN).putInt(1).putInt(responseBody.length)
					.put(responseBody).array());
			return response;
		} else {
			Response response = new Response(op, 4, request.getId());
			response.setCount(1);
			return response;
		}
	}

	private void update(int spaceNum, ByteBuffer buffer, Tuple tuple) {
		int fieldNo = buffer.getInt();
		Updates up = Updates.valueOf((int) buffer.get());

		Tuple args = null;
		if (up.args > 0) {
			args = Tuple.createFromPackedFields(buffer, ByteOrder.LITTLE_ENDIAN, 1);
		}
		if (up.args > 1) {
			args = Tuple.createFromPackedFields(ByteBuffer.wrap(args.getBytes(0)), ByteOrder.LITTLE_ENDIAN, up.args);
		}
		Space space = spaces.get(spaceNum);
		Index primary = space.indexes.get(0);
		Tuple stored = primary.getOne(tuple);
		if (stored != null) {
			if (stored.size() < fieldNo || fieldNo < 0) {
				throw new TarantoolException(54, String.format("Field %d was not found in the tuple", fieldNo));
			}
			if (up == Updates.ADD || up == Updates.AND || up == Updates.XOR || up == Updates.OR || up == Updates.MAX || up == Updates.SUB) {
				int storedFieldLength = stored.getBytes(fieldNo).length;
				if (storedFieldLength == 4) {
					stored.setInt(fieldNo, (int) arithmeticUpdate(up, stored.getInt(fieldNo), args.getInt(0)));
				} else if (storedFieldLength == 8) {
					stored.setLong(fieldNo, arithmeticUpdate(up, stored.getLong(fieldNo), args.getBytes(0).length == 4 ? args.getInt(0) : args.getLong(0)));
				} else {
					throw new TarantoolException(40, String.format("Field type does not match one required by operation: expected a %s", "NUM or NUM 64"));
				}

			} else if (up == Updates.DELETE) {
				stored = deleteField(fieldNo, stored);
				if (stored.size() < 2) {
					throw new TarantoolException(25, "UPDATE error: the new tuple has no fields");
				}
			} else if (up == Updates.INSERT) {
				stored = insertField(fieldNo, args, stored);
			} else if (up == Updates.SPLICE) {
				splice(fieldNo, args, stored);
			} else if (up == Updates.SET) {
				stored.setBytes(fieldNo, args.getBytes(0));
			}
			delete(spaceNum, tuple);
			put(spaceNum, stored, true, false);
		}
	}

	/**
	 * <p>
	 * splice.
	 * </p>
	 * 
	 * @param fieldNo
	 *            a int.
	 * @param args
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @param stored
	 *            a {@link org.tarantool.core.Tuple} object.
	 */
	public void splice(int fieldNo, Tuple args, Tuple stored) {
		byte[] fieldValue = stored.getBytes(fieldNo);
		int from = args.getInt(0);
		int len = args.getInt(1);
		byte[] insert = args.getBytes(2);
		ByteBuffer resultBuf = ByteBuffer.allocate(fieldValue.length - len + insert.length).order(ByteOrder.LITTLE_ENDIAN);
		stored.setBytes(fieldNo,
				resultBuf.put(Arrays.copyOfRange(fieldValue, 0, from)).put(insert).put(Arrays.copyOfRange(fieldValue, from + len, fieldValue.length)).array());
	}

	/**
	 * <p>
	 * insertField.
	 * </p>
	 * 
	 * @param fieldNo
	 *            a int.
	 * @param args
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @param stored
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple insertField(int fieldNo, Tuple args, Tuple stored) {
		Tuple copy = new Tuple(stored.size() + 1);
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

	/**
	 * <p>
	 * deleteField.
	 * </p>
	 * 
	 * @param fieldNo
	 *            a int.
	 * @param stored
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple deleteField(int fieldNo, Tuple stored) {
		Tuple copy = new Tuple(stored.size() - 1);
		for (int i = 0, offset = 0; i < copy.size(); i++) {
			if (i == fieldNo) {
				offset = 1;
			}
			copy.setBytes(i, stored.getBytes(i + offset));
		}
		return copy;
	}

	/**
	 * <p>
	 * arithmeticUpdate.
	 * </p>
	 * 
	 * @param up
	 *            a {@link org.tarantool.core.proto.Updates} object.
	 * @param value
	 *            a long.
	 * @param arg
	 *            a long.
	 * @return a long.
	 */
	public long arithmeticUpdate(Updates up, long value, long arg) {
		if (up == Updates.ADD)
			value += arg;
		else if (up == Updates.AND)
			value &= arg;
		else if (up == Updates.XOR)
			value ^= arg;
		else if (up == Updates.OR)
			value |= arg;
		else if (up == Updates.SUB)
			value -= arg;
		else if (up == Updates.MAX)
			value = Math.max(value, arg);
		return value;
	}

	/**
	 * <p>
	 * copy.
	 * </p>
	 * 
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @param fields
	 *            a int.
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple copy(Tuple tuple, int... fields) {
		Tuple t = new Tuple(fields.length);
		for (int i = 0; i < fields.length; i++) {
			t.setBytes(i, tuple.getBytes(fields[i]));
		}
		return t;
	}

	public Tuple copyExcept(Tuple tuple, Integer... fields) {
		Tuple t = new Tuple(tuple.size() - fields.length);
		List<Integer> except = Arrays.asList(fields);
		for (int i = 0, j = 0; i < tuple.size(); i++) {
			if (!except.contains(i)) {
				t.setBytes(j++, tuple.getBytes(i));
			}
		}
		return t;
	}

	public Map<String, CallStub> getCalls() {
		return calls;
	}

	public Map<Integer, Space> getSpaces() {
		return spaces;
	}

}
