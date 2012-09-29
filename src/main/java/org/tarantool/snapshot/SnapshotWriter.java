package org.tarantool.snapshot;

import static org.tarantool.snapshot.Const.ROW_DATA_HEADER_SIZE;
import static org.tarantool.snapshot.Const.SNAP_HEADER;
import static org.tarantool.snapshot.Const.SNAP_TAG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import org.tarantool.core.Tuple;

/**
 * <p>
 * SnapshotWriter class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class SnapshotWriter extends TupleWriter {

	protected ByteBuffer marker;

	/**
	 * <p>
	 * Constructor for SnapshotWriter.
	 * </p>
	 * 
	 * @param channel
	 *            a {@link java.nio.channels.WritableByteChannel} object.
	 * @throws java.io.IOException
	 *             if any.
	 */
	public SnapshotWriter(WritableByteChannel channel) throws IOException {
		super(channel, SNAP_TAG);
		ByteBuffer header = ByteBuffer.wrap(SNAP_HEADER).order(ByteOrder.LITTLE_ENDIAN);
		header.position(header.capacity());
		flipAndWriteFully(header);
		this.marker = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Const.ROW_START_MARKER);
	}

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
	public void writeRow(int space, Tuple tuple) throws IOException {
		flipAndWriteFully(marker);
		int sz = tuple.calcFieldsSize();
		ByteBuffer buffer = ByteBuffer.allocate(ROW_DATA_HEADER_SIZE + sz).order(ByteOrder.LITTLE_ENDIAN);
		buffer.putShort(tag);// tag
		buffer.putLong(0L);// cookie
		buffer.putInt(space);// space
		buffer.putInt(tuple.size());// cardinality
		buffer.putInt(sz);// size of packed fields
		tuple.packFields(buffer);// pack field to buffer
		ByteBuffer header = createHeader(buffer.array());
		flipAndWriteFully(header);
		flipAndWriteFully(buffer);
	}

}
