package org.tarantool.snapshot;

import static org.tarantool.snapshot.Const.EOF_MARKER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

public class TupleWriter {

	protected WritableByteChannel channel;
	protected short tag;

	protected TupleWriter(WritableByteChannel channel, short tag) {
		super();
		this.channel = channel;
		this.tag = tag;
	}

	protected void flipAndWriteFully(ByteBuffer buffer) throws IOException {
		buffer.flip();
		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
	}

	/**
	 * <p>
	 * getTm.
	 * </p>
	 * 
	 * @return a double.
	 */
	protected double getTm() {
		return System.currentTimeMillis();
	}

	protected AtomicLong lsn = new AtomicLong(0L);

	protected ByteBuffer createHeader(byte[] array) {
		ByteBuffer buf = ByteBuffer.allocate(Const.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
		buf.position(4);
		buf.putLong(getLSN());
		buf.putDouble(getTm());
		buf.putInt(array.length);
		buf.putInt(IntelCrc32c.crc32cSb864bitLE(0L, array, 0, array.length));
		buf.putInt(0, IntelCrc32c.crc32cSb864bitLE(0L, buf.array(), 4, Const.HEADER_SIZE - 4));
		buf.position(buf.capacity());
		return buf;

	}

	protected long getLSN() {
		return lsn.incrementAndGet();
	}

	/**
	 * <p>
	 * close.
	 * </p>
	 * 
	 * @throws java.io.IOException
	 *             if any.
	 */
	public void close() throws IOException {
		flipAndWriteFully(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(EOF_MARKER));
		channel.close();
	}

}
