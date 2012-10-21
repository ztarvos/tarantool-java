package org.tarantool.snapshot;

import static org.tarantool.snapshot.Const.XLOG_HEADER;
import static org.tarantool.snapshot.Const.XLOG_TAG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import org.tarantool.core.cmd.DMLRequest;

/**
 * <p>
 * SnapshotWriter class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class XLogWriter extends TupleWriter {

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
	public XLogWriter(WritableByteChannel channel) throws IOException {
		super(channel, XLOG_TAG);
		ByteBuffer header = ByteBuffer.wrap(XLOG_HEADER).order(ByteOrder.LITTLE_ENDIAN);
		header.position(header.capacity());
		flipAndWriteFully(header);
		this.marker = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(Const.ROW_START_MARKER);
	}

	protected ByteBuffer marker;

	public <T extends DMLRequest<T>> void writeXLog(DMLRequest<T> request) throws IOException {
		byte[] body = request.getBody();
		ByteBuffer buffer = ByteBuffer.allocate(body.length + 20).order(ByteOrder.LITTLE_ENDIAN);
		buffer.putShort(Const.XLOG_TAG);
		buffer.putLong(getCookie());
		buffer.putShort((short) request.getOp());
		buffer.putInt(request.getSpace());
		buffer.putInt(request.getFlags());
		buffer.put(body);
		ByteBuffer header = createHeader(buffer.array());
		flipAndWriteFully(marker);
		flipAndWriteFully(header);
		flipAndWriteFully(buffer);
	}

	protected long getCookie() {
		return 0L;
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

}
