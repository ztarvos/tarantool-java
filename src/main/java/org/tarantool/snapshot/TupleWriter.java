package org.tarantool.snapshot;

import static org.tarantool.snapshot.Const.DATA_HEADER_SIZE;
import static org.tarantool.snapshot.Const.EOF_MARKER;
import static org.tarantool.snapshot.Const.ROW_HEADER_SIZE;
import static org.tarantool.snapshot.Const.ROW_START_MARKER;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.tarantool.core.Tuple;

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

	/**
	 * <p>
	 * write.
	 * </p>
	 * 
	 * @param space
	 *            a int.
	 * @param tuple
	 *            a {@link org.tarantool.core.Tuple} object.
	 * @throws java.io.IOException
	 *             if any.
	 */
	public void write(int space, Tuple tuple) throws IOException {

		int sz = tuple.calcFieldsSize();
		ByteBuffer buffer = ByteBuffer.allocate(ROW_HEADER_SIZE + DATA_HEADER_SIZE + sz).order(ByteOrder.LITTLE_ENDIAN);
		buffer.position(ROW_HEADER_SIZE);// first fill data
		buffer.putShort(tag);// tag
		buffer.putLong(0L);// cookie
		buffer.putInt(space);// space
		buffer.putInt(tuple.size());// cardinality
		buffer.putInt(sz);// size of packed fields
		tuple.packFields(buffer);// pack field to buffer
		buffer.position(ROW_HEADER_SIZE - 4);// move to data crc32 pos
		// put data crc
		buffer.putInt(IntelCrc32c.crc32cSb864bitLE(0L, buffer.array(), ROW_HEADER_SIZE, DATA_HEADER_SIZE + sz));
		buffer.position(0);// move to start
		buffer.putInt(ROW_START_MARKER);
		buffer.putInt(0);// skip header crc32
		buffer.putLong(lsn.incrementAndGet());// lsn
		buffer.putDouble(getTm());// put time in millis
		buffer.putInt(sz + DATA_HEADER_SIZE);// put len of data part
		buffer.position(4);// move to header crc32
		buffer.putInt(IntelCrc32c.crc32cSb864bitLE(0L, buffer.array(), 8, 24));// calculate
																				// header
																				// crc32
		buffer.position(ROW_HEADER_SIZE + DATA_HEADER_SIZE + sz);// set limit to
																	// end
		flipAndWriteFully(buffer);
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
