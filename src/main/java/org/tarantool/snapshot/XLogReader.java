package org.tarantool.snapshot;

import java.io.EOFException;
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
public class XLogReader extends TupleReader {

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
	public XLogReader(ReadableByteChannel channel) throws IOException {
		super(channel, Const.XLOG_TAG);
		ByteBuffer xlogHeader = ByteBuffer.allocate(Const.XLOG_HEADER.length).order(ByteOrder.LITTLE_ENDIAN);
		readFullyAndFlip(xlogHeader);
		if (!Arrays.equals(Const.XLOG_HEADER, xlogHeader.array())) {
			throw new IllegalStateException("Snapshot file should have header " + new String(Const.XLOG_HEADER));
		}
	}

	public static class XLogEntry {
		public Header header;
		public short tag;
		public long cookie;
		public int op;
		public int space;
		public int flags;

		@Override
		public String toString() {
			return "XLogEntry [header=" + header + ", tag=" + tag + ", cookie=" + cookie + ", op=" + op + ", space=" + space + ", flags=" + flags + "]";
		}

		public Tuple tuple;
	}

	protected XLogEntry readEntry() throws IOException {
		XLogEntry cmd = new XLogEntry();
		cmd.header = readHeader();
		ByteBuffer body = readBody(cmd.header);
		cmd.tag = body.getShort();
		cmd.cookie = body.getLong();
		cmd.op = body.getShort();
		cmd.space = body.getInt();
		cmd.flags = body.getInt();
		cmd.tuple = Tuple.create(body, ByteOrder.LITTLE_ENDIAN);
		return cmd;
	}

	public XLogEntry nextEntry() throws IOException {
		try {
			if (hasNext()) {
				return readEntry();
			} else {
				return null;
			}
		} catch (EOFException eof) {
			return null;
		}

	}

}
