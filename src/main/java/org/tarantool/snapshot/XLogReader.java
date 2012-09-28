package org.tarantool.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;

/**
 * <p>
 * SnapShotReader class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class XLogReader extends TupleReader{
	

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
		super(channel,Const.XLOG_TAG);
		ByteBuffer xlogHeader = ByteBuffer.allocate(Const.XLOG_HEADER.length).order(ByteOrder.LITTLE_ENDIAN);
		readFullyAndFlip(xlogHeader);
		if (!Arrays.equals(Const.XLOG_HEADER, xlogHeader.array())) {
			throw new IllegalStateException("Snapshot file should have header " + new String(Const.XLOG_HEADER));
		}
	}


}
