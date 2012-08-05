package org.tarantool.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;


import org.tarantool.core.Const.OP;
import org.tarantool.core.exception.CommunicationException;

public class Output {

	public void update(int space, int flag, Tuple id, byte[]... ops)  {
		byte[] bytes = id.pack();
		int count = ops == null ? 0 : ops.length;
		int capacity = 8 + bytes.length + 4;
		if (ops != null)
			for (byte[] op : ops) {
				capacity += op.length;
			}
		buffer = packet(Const.OP.UPDATE, capacity).putInt(space).putInt(flag).put(bytes);
		buffer.putInt(count);
		if (ops != null) {
			for (byte[] op : ops) {
				buffer.put(op);
			}
		}
		write();
	}

	static final int REQUEST_HEADER_SIZE = 12;
	AtomicInteger id;

	ByteBuffer buffer;
	WritableByteChannel channel;

	public Output(WritableByteChannel channel, AtomicInteger id) {
		this.channel = channel;
		this.id = id;
	}

	ByteBuffer packet(Const.OP op, int capacity) {
		ByteBuffer buf = allocate(REQUEST_HEADER_SIZE + capacity);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putInt(op.type).putInt(capacity).putInt(id.getAndIncrement());
		return buf;
	}

	ByteBuffer allocate(int size) {
		return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
	}

	public void insert(int space, int flag, Tuple tuple) {
		byte[] bytes = tuple.pack();
		int capacity = 8 + bytes.length;
		buffer = packet(Const.OP.INSERT, capacity).putInt(space).putInt(flag).put(bytes);
		write();
	}

	public void delete(int space, int flag, Tuple id)  {
		byte[] bytes=id.pack();
		int capacity = 8 + bytes.length;
		buffer = packet(Const.OP.DELETE, capacity).putInt(space).putInt(flag).put(bytes);
		write();

	}



	public void select(int space, int index, int offset, int limit, Tuple... tuples) {
		int size = 0;
		byte[][] packed = new byte[tuples.length][];
		for (int i = 0, e = tuples.length; i < e; i++) {
			packed[i] = tuples[i].pack();
			size += packed[i].length;
		}
		int capacity = 16 + 4 + size;
		buffer = packet(Const.OP.SELECT, capacity).putInt(space).putInt(index).putInt(offset).putInt(limit).putInt(tuples.length);
		for (byte[] tuple : packed) {
			buffer.put(tuple);
		}
		write();
	}

	private void write()  {
		buffer.flip();
		while (buffer.hasRemaining()) {
			try {
				channel.write(buffer);
			} catch (IOException e) {
				throw new CommunicationException("Can't write buffer",e);
			}
		}
	}

	public void ping()  {
		buffer = packet(OP.PING, 0);
		write();

	}

}
