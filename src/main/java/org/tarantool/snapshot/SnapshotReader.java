package org.tarantool.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

import org.tarantool.core.Tuple;

/**
 * <p>
 * SnapShotReader class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class SnapshotReader extends TupleReader {

	/**
	 * <p>
	 * Constructor for SnapShotReader.
	 * </p>
	 * 
	 * @param channel
	 *            a {@link java.nio.channels.ReadableByteChannel} object.
	 * @throws java.io.IOException
	 *             if any.
	 */
	public SnapshotReader(ReadableByteChannel channel) throws IOException {
		super(channel, Const.SNAP_TAG);
		ByteBuffer snapHeader = ByteBuffer.allocate(Const.SNAP_HEADER.length).order(ByteOrder.LITTLE_ENDIAN);
		readFullyAndFlip(snapHeader);
		if (!Arrays.equals(Const.SNAP_HEADER, snapHeader.array())) {
			throw new IllegalStateException("Snapshot file should have header " + new String(Const.SNAP_HEADER));
		}
	}

	public Row nextRow() throws IOException {
		if (hasNext()) {
			Header header = readHeader();
			return readRow(header);
		} else {
			return null;
		}
	}

	public static class Row {
		public Header header;
		public short tag;
		public long cookie;
		public int space;
		public int cardinality;
		public int size;
		public Tuple data;// create this tuple only from packed fields
	}

	protected Row readRow(Header header) throws IOException {
		ByteBuffer body = readBody(header);
		Row row = new Row();
		row.header = header;
		row.tag = body.getShort();
		if (row.tag != tag) {
			throw new IllegalStateException("Row should has valid tag, actual " + row.tag + " excepted " + tag);
		}
		row.cookie = body.getLong();
		row.space = body.getInt();
		row.cardinality = body.getInt();
		row.size = body.getInt();
		body.limit(body.capacity());
		row.data = Tuple.createFromPackedFields(body, ByteOrder.LITTLE_ENDIAN, row.cardinality);

		if (row.data.size() != row.cardinality) {
			throw new IllegalStateException("Row size is " + row.cardinality + " but tuple has " + row.data.size());
		}

		return row;
	}

}
