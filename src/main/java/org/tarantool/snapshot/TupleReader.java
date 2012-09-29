package org.tarantool.snapshot;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;

public class TupleReader {

	protected ReadableByteChannel channel;
	protected short tag;

	protected TupleReader(ReadableByteChannel channel, short tag) {
		super();
		this.channel = channel;
		this.tag = tag;
		this.header = ByteBuffer.allocate(Const.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
		this.marker = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
	}

	protected ByteBuffer readFullyAndFlip(ByteBuffer buf) throws IOException {
		int read = 0;
		while (buf.remaining() > 0 && (read = channel.read(buf)) > 0) {
		}
		if (read == 0) {
			throw new EOFException();
		}
		buf.flip();
		return buf;
	}

	protected ByteBuffer header;

	protected ByteBuffer marker;

	public class Header {
		public int headerCrc32;
		public long lsn;
		public double tm;
		public int len;
		public int dataCrc32;

		@Override
		public String toString() {
			return "Header [headerCrc32=" + headerCrc32 + ", lsn=" + lsn + ", tm=" + tm + ", len=" + len + ", dataCrc32=" + dataCrc32 + "]";
		}

	}

	protected ByteBuffer readBody(Header header) throws IOException {
		ByteBuffer body = ByteBuffer.allocate(header.len).order(ByteOrder.LITTLE_ENDIAN);
		readFullyAndFlip(body);
		if (header.dataCrc32 != IntelCrc32c.crc32cSb864bitLE(0L, body.array(), 0, header.len)) {
			throw new IllegalStateException("Data crc32 mismatch");
		}
		return body;
	}

	protected Header readHeader() throws IOException {
		header.clear();
		readFullyAndFlip(header);
		Header head = new Header();
		head.headerCrc32 = header.getInt();
		head.lsn = header.getLong();
		head.tm = header.getDouble();
		head.len = header.getInt();
		head.dataCrc32 = header.getInt();
		if (IntelCrc32c.crc32cSb864bitLE(0L, header.array(), 4, 24) != head.headerCrc32) {
			throw new IllegalStateException("Headers crc32 mismatch");
		}
		return head;
	}

	protected boolean hasNext() throws IOException {
		marker.clear();
		readFullyAndFlip(marker);
		int marker = this.marker.getInt();
		if (marker != Const.ROW_START_MARKER) {
			throw new IllegalStateException("Row should starts with ROW_START_MARKER but has " + Integer.toHexString(marker));
		}
		if (marker == Const.EOF_MARKER) {
			return false;
		}
		return true;
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
