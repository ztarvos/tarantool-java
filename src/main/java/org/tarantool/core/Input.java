package org.tarantool.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.exception.TarantoolException;

public class Input {
	static final int RESPONSE_HEADER_SIZE = 16;
	static final int PING_HEADER_SIZE = 12;
	AtomicInteger id;
	ReadableByteChannel channel;
	ByteBuffer buffer;

	public Input(ReadableByteChannel channel, AtomicInteger id) {
		this.channel = channel;
		this.id = id;
	}

	public void read() {
		int res = 0;
		try {
			while (buffer.hasRemaining() && (res = channel.read(buffer)) > -1) {
			}
		} catch (IOException e) {
			throw new CommunicationException("Can't read data", e);
		}
		if (res == -1) {
			throw new CommunicationException("Connection lost");
		}
		buffer.flip();
	}

	private Object readResult() {
		buffer = ByteBuffer.allocate(RESPONSE_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
		read();
		Object o = HEADER;
		try {
			while ((o = ((Callable<?>) o).call()) instanceof Callable<?>) {
				read();
			}
		} catch (TarantoolException e) {
			throw e;
		} catch (Exception e) {
			throw new CommunicationException("Can't read result", e);
		}
		return o;
	}

	@SuppressWarnings("unchecked")
	public List<Tuple> readTuples() {
		Object res = readResult();
		return Integer.valueOf(0).equals(res)?new ArrayList<Tuple>():(List<Tuple>) res;
	}

	public Integer readCount() {
		return (Integer) readResult();
	}

	public Tuple readTuple() {
		List<Tuple> tuples = readTuples();
		return tuples.isEmpty() ? null : tuples.get(0);
	}

	final Callable<Integer> COUNT_RESPONSE = new Callable<Integer>() {

		public Integer call() throws Exception {
			return buffer.getInt();
		}
	};

	final Callable<String> ERROR_RESPONSE = new Callable<String>() {

		public String call() throws Exception {
			int code = buffer.getInt();
			byte[] message = new byte[buffer.capacity() - 4];
			buffer.get(message);
			throw new TarantoolException(code, new String(message));
		}
	};

	final Callable<List<Tuple>> TUPLE_RESPONSE = new Callable<List<Tuple>>() {

		public List<Tuple> call() throws Exception {
			int count = buffer.getInt();
			List<Tuple> tuples = new ArrayList<Tuple>(count);
			for (int j = 0; j < count; j++) {
				tuples.add(Tuple.createFQ(buffer, ByteOrder.LITTLE_ENDIAN));
			}
			return tuples;
		}
	};

	final Callable<Callable<?>> HEADER = new Callable<Callable<?>>() {

		public Callable<?> call() throws Exception {
			int op = buffer.getInt();
			int size = buffer.getInt();
			int id = buffer.getInt();
			checkAnswerId(id);
			int ret = buffer.getInt();
			if (ret != 0) {
				buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
				buffer.putInt(ret);
				return ERROR_RESPONSE;
			}
			buffer = ByteBuffer.allocate(size - 4).order(ByteOrder.LITTLE_ENDIAN);
			if (op == Const.OP.INSERT.type || op == Const.OP.UPDATE.type || op == Const.OP.DELETE.type) {
				if (size > 8) {
					return TUPLE_RESPONSE;
				} else {
					return COUNT_RESPONSE;
				}
			}
			if (op == Const.OP.SELECT.type) {
				return TUPLE_RESPONSE;
			}

			throw new IllegalStateException("Unknown answer: " + "op: " + op + " size: " + size + " id: " + id + " ret: " + ret);
		}
	};

	@SuppressWarnings("unused")
	public Boolean ping() {
		buffer = ByteBuffer.allocate(PING_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
		read();
		int op = buffer.getInt();
		int size = buffer.getInt();
		int id = buffer.getInt();
		checkAnswerId(id);
		return true;

	}

	private void checkAnswerId(int id) {
		int answerId = Input.this.id.get() - 1;
		if (id != answerId) {
			throw new IllegalStateException("Answer should have id " + answerId + " but have " + id);
		}
	}

}
