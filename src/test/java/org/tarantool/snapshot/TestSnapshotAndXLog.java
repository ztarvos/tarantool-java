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
import org.tarantool.core.cmd.Insert;
import org.tarantool.facade.TupleSupport;
import org.tarantool.pool.SocketChannelPooledConnectionFactory;
import org.tarantool.snapshot.SnapshotReader.Row;
import org.tarantool.snapshot.XLogReader.XLogEntry;

public class TestSnapshotAndXLog {
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

			@Override
			protected long getLSN() {
				return 0L;
			}
		};
		TupleSupport ts = new TupleSupport();
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			snapshot.writeRow(0, tuple);
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
	public void testXLogCreator() throws IOException {
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
		XLogWriter xlog = new XLogWriter(channel) {
			long lsn = 2;
			int i = 0;
			double[] tm = { 1.3489211051619418E9, 1.3489211051632116E9, 1.3489211051634953E9, 1.3489211051636925E9, 1.3489211051638768E9, 1.3489211051640718E9,
					1.3489211051642623E9, 1.3489211051644433E9, 1.3489211051646378E9, 1.3489211051648207E9 };

			@Override
			protected double getTm() {
				return tm[i++];
			}

			@Override
			protected long getLSN() {
				return lsn++;
			}

			@Override
			protected long getCookie() {
				return 72058143204835330L;
			}
		};
		TupleSupport ts = new TupleSupport();
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			xlog.writeXLog(new Insert(0, tuple).flags(2));
		}

		xlog.close();
		Assert.assertTrue(closed.get());
		DataInputStream is = new DataInputStream(ClassLoader.getSystemResourceAsStream("test.xlog"));
		byte[] b = new byte[os.size()];
		is.readFully(b);
		Assert.assertTrue(is.available() == 0);
		is.close();
		Assert.assertTrue(Arrays.equals(b, os.toByteArray()));
	}

	@Test
	public void testSnapReader() throws IOException {

		final AtomicBoolean closed = new AtomicBoolean(false);
		byte[] ar = readFile("test.snap");
		ReadableByteChannel readableByteChannel = createReadableByteChannel(closed, ar);
		TupleSupport ts = new TupleSupport();
		SnapshotReader snapShotReader = new SnapshotReader(readableByteChannel);
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			Row row = snapShotReader.nextRow();
			Assert.assertTrue(Arrays.equals(tuple.pack(), row.data.pack()));
		}
		snapShotReader.close();
		Assert.assertTrue(closed.get());

	}

	@Test
	public void testXLogReader() throws IOException {

		final AtomicBoolean closed = new AtomicBoolean(false);
		byte[] ar = readFile("test.xlog");
		ReadableByteChannel readableByteChannel = createReadableByteChannel(closed, ar);
		TupleSupport ts = new TupleSupport();
		XLogReader xlogReader = new XLogReader(readableByteChannel);
		for (int i = 0; i < 10; i++) {
			Tuple tuple = ts.create(i, Long.parseLong("98765432" + i), "Hello world " + i + "!");
			XLogEntry entry = xlogReader.nextEntry();
			Assert.assertTrue(Arrays.equals(tuple.pack(), entry.tuple.pack()));
		}
		xlogReader.close();
		Assert.assertTrue(closed.get());

	}

	protected byte[] readFile(String name) throws IOException {
		DataInputStream is = new DataInputStream(ClassLoader.getSystemResourceAsStream(name));
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		while (is.available() > 0) {
			os.write(is.readByte());
		}
		is.close();
		byte[] ar = os.toByteArray();
		return ar;
	}

	protected ReadableByteChannel createReadableByteChannel(final AtomicBoolean closed, byte[] ar) {
		final ByteBuffer source = ByteBuffer.wrap(ar);
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
		return readableByteChannel;
	}

	
}
