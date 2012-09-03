package org.tarantool.snapshot;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.junit.Test;
import org.tarantool.core.TarantoolConnection;
import org.tarantool.core.Tuple;
import org.tarantool.facade.TupleSupport;
import org.tarantool.pool.SocketChannelPooledConnectionFactory;
import org.tarantool.snapshot.SnapshotReader.Row;

public class TestSnapshot {
	// @Test
	public void insertTestTuples() {
		SocketChannelPooledConnectionFactory factory = new SocketChannelPooledConnectionFactory();
		TarantoolConnection connection = factory.getConnection();
		TupleSupport ts = new TupleSupport();
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			connection.insert(0, tuple);
		}
		connection.close();
	}

	@Test
	public void testSnapCreator() throws IOException {
		final AtomicBoolean closed = new AtomicBoolean(false);
		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		WritableByteChannel channel = new WritableByteChannel() {

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void close() throws IOException {
				closed.set(true);

			}

			@Override
			public int write(ByteBuffer src) throws IOException {
				int len = src.limit() - src.position();
				os.write(src.array(), src.position(), len);
				src.position(src.limit());
				return len;

			}
		};
		SnapshotWriter snapshot = new SnapshotWriter(channel) {
			@Override
			protected double getTm() {
				return Double.longBitsToDouble(0x41D407B41F76243EL);
			}
		};
		TupleSupport ts = new TupleSupport();
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			snapshot.write(0, Const.SNAP_TAG, tuple);
		}

		snapshot.close();
		Assert.assertTrue(closed.get());
		DataInputStream is = new DataInputStream(ClassLoader.getSystemResourceAsStream("test.snap"));
		byte[] b = new byte[os.size()];
		is.readFully(b);
		Assert.assertTrue(is.available() == 0);
		is.close();
		Assert.assertTrue(Arrays.equals(b, os.toByteArray()));
	}

	@Test
	public void testSnapReader() throws IOException {

		final AtomicBoolean closed = new AtomicBoolean(false);
		DataInputStream is = new DataInputStream(ClassLoader.getSystemResourceAsStream("test.snap"));
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		while (is.available() > 0) {
			os.write(is.readByte());
		}
		is.close();
		final ByteBuffer source = ByteBuffer.wrap(os.toByteArray());
		ReadableByteChannel readableByteChannel = new ReadableByteChannel() {

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void close() throws IOException {
				closed.set(true);

			}

			@Override
			public int read(ByteBuffer dst) throws IOException {
				int rem = Math.min(source.remaining(), dst.remaining());
				dst.put(source.array(), source.position(), rem);
				source.position(source.position() + rem);
				return rem;
			}
		};
		TupleSupport ts = new TupleSupport();
		SnapshotReader snapShotReader = new SnapshotReader(readableByteChannel);
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			Row row = snapShotReader.readNext();
			Assert.assertTrue(Arrays.equals(tuple.pack(), row.data.pack()));
		}
		snapShotReader.close();
		Assert.assertTrue(closed.get());

	}
}
