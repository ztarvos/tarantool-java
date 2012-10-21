package org.tarantool.snapshot;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.tarantool.core.Tuple;
import org.tarantool.core.cmd.Update;
import org.tarantool.core.impl.OperationImpl;

/**
 * <p>
 * SnapShotReader class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class XLogReader extends TupleReader {

	protected XLogReader(ReadableByteChannel channel, boolean readHeader) throws IOException {
		super(channel, Const.XLOG_TAG);
		if (readHeader) {
			readAndCheckFileHeader();
		}
	}

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
		readAndCheckFileHeader();
	}

	protected void readAndCheckFileHeader() throws IOException {
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
		public List<OperationImpl> ops;
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
		if (cmd.op == Update.OP_CODE) {
			int operations = body.getInt();
			List<OperationImpl> ops = new ArrayList<OperationImpl>();
			for (int i = 0; i < operations; i++) {
				ops.add(OperationImpl.unpack(body));
			}
			cmd.ops = ops;
		}
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
