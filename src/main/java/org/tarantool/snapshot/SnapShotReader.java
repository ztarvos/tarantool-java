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
public class SnapShotReader {
	ReadableByteChannel channel;

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
	public SnapShotReader(ReadableByteChannel channel) throws IOException {
		this.channel = channel;
		ByteBuffer snapHeader = ByteBuffer.allocate(Const.HEADER.length).order(ByteOrder.LITTLE_ENDIAN);
		readFullyAndFlip(snapHeader);
		if (!Arrays.equals(Const.HEADER, snapHeader.array())) {
			throw new IllegalStateException("Snapshot file should have header " + new String(Const.HEADER));
		}
	}

	private ByteBuffer readFullyAndFlip(ByteBuffer buf) throws IOException {
		while (buf.remaining() > 0 && channel.read(buf) > 0) {

		}
		buf.flip();
		return buf;
	}

	ByteBuffer headers = ByteBuffer.allocate(Const.DATA_HEADER_SIZE + Const.ROW_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

	public class Row {
		public int marker;
		public int headerCrc32;
		public long lsn;
		public double tm;
		public int len;
		public int dataCrc32;
		public short tag;
		public long cookie;
		public int space;
		public int cardinality;
		public int size;
		public Tuple data;// create this tuple only from packed fields
	}

	/**
	 * <p>
	 * readNext.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.snapshot.SnapShotReader.Row} object.
	 * @throws java.io.IOException
	 *             if any.
	 */
	public Row readNext() throws IOException {
		headers.clear();
		readFullyAndFlip(headers);
		if (headers.position() == 4) {
			if (headers.getInt() != Const.EOF_MARKER) {
				throw new IllegalStateException("Snapshot should ends with EOF_MARKER");
			} else {
				return null;
			}
		}
		Row row = new Row();
		row.marker = headers.getInt();
		if (row.marker != Const.ROW_START_MARKER) {
			throw new IllegalStateException("Row should starts with ROW_START_MARKER");
		}
		row.headerCrc32 = headers.getInt();
		row.lsn = headers.getLong();
		row.tm = headers.getDouble();
		row.len = headers.getInt();
		row.dataCrc32 = headers.getInt();
		if (IntelCrc32c.crc32cSb864bitLE(0L, headers.array(), 8, 24) != row.headerCrc32) {
			throw new IllegalStateException("Headers crc32 mismatch");
		}
		row.tag = headers.getShort();
		if (row.tag != Const.SNAP_TAG) {
			throw new IllegalStateException("Row should has SNAP tag");
		}
		row.cookie = headers.getLong();
		row.space = headers.getInt();
		row.cardinality = headers.getInt();
		row.size = headers.getInt();

		ByteBuffer dataBuff = ByteBuffer.allocate(row.size + Const.ROW_HEADER_SIZE + Const.DATA_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
		headers.flip();
		dataBuff.put(headers);

		readFullyAndFlip(dataBuff);
		if (row.dataCrc32 != IntelCrc32c.crc32cSb864bitLE(0L, dataBuff.array(), Const.ROW_HEADER_SIZE, row.size + Const.DATA_HEADER_SIZE)) {
			throw new IllegalStateException("Data crc32 mismatch");
		}
		dataBuff.position(Const.ROW_HEADER_SIZE + Const.DATA_HEADER_SIZE);
		dataBuff.limit(dataBuff.capacity());
		row.data = Tuple.createFromPackedFields(dataBuff, ByteOrder.LITTLE_ENDIAN, row.cardinality);

		if (row.data.size() != row.cardinality) {
			throw new IllegalStateException("Row size is " + row.cardinality + " but tuple has " + row.data.size());
		}

		return row;
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
		channel.close();
	}
}
