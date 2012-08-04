package org.tarantool.facade;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import org.tarantool.core.IntelCrc32c;
import org.tarantool.core.Tuple;

public class Snapshot {
	FileChannel channel;

	byte[] HEADER = "SNAP\n0.11\n\n".getBytes();

	@SuppressWarnings("resource")
	public Snapshot(String filename) throws IOException {
		File file = new File(filename);
		channel = new FileOutputStream(file).getChannel();
		push(ByteBuffer.wrap(HEADER).order(ByteOrder.LITTLE_ENDIAN));

	}

	private void push(ByteBuffer buffer) throws IOException {
		buffer.rewind();
		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
	}

	private class Header {
		int headerCrc32c;
		long lsn = 0L;
		double tm = System.currentTimeMillis();
		int len;
		int dataCrc32c;
	}

	private class Row {
		int marker = 0xba0babed;
		Header header = new Header();
		short tag = (short) 0xFFFF;// SNAP tag
		long cookie = 0L;

	}

	public void write(int space, Tuple tuple) throws IOException {

		Row row = new Row();
		int sz = tuple.calcFieldsSize();
		ByteBuffer packedTuple = ByteBuffer.allocate(sz + 12).order(ByteOrder.LITTLE_ENDIAN);
		packedTuple.putInt(space);
		packedTuple.putInt(tuple.size());
		packedTuple.putInt(sz);
		tuple.packFields(packedTuple);

		row.header.len = packedTuple.array().length + 2 + 8;
		ByteBuffer data = ByteBuffer.allocate(row.header.len).order(ByteOrder.LITTLE_ENDIAN);
		data.putShort(row.tag).putLong(row.cookie).put(packedTuple.array());

		row.header.dataCrc32c = (int) IntelCrc32c.crc32cSb864bitLE(0, data.array());

		ByteBuffer header = ByteBuffer.allocate(8 + 8 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
		header.putLong(row.header.lsn).putDouble(row.header.tm).putInt(row.header.len).putInt(row.header.dataCrc32c);
		;
		row.header.headerCrc32c = (int) IntelCrc32c.crc32cSb864bitLE(0, header.array());

		ByteBuffer rowBuffer = ByteBuffer.allocate(8 + header.array().length + data.array().length).order(ByteOrder.LITTLE_ENDIAN).putInt(row.marker)
				.putInt(row.header.headerCrc32c).put(header.array()).put(data.array());
		push(rowBuffer);
	}

	public void close() throws IOException {
		push(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0x10adab1e));
		channel.close();
	}

	public static void main(String[] args) throws IOException {

		Snapshot snapshot = new Snapshot("00000000000000000001.snap");
		TupleSupport ts = new TupleSupport();
		for (int i = 0; i < 1000000; i++) {
			Tuple tuple = ts.create(i, 987654321L, "Hello world :)");
			snapshot.write(0, tuple);
		}

		snapshot.close();
		;
	}

}
