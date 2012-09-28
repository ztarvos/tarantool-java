package org.tarantool.snapshot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ByteChannel;

import org.tarantool.core.Tuple;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.impl.ByteChannelTransport;

public class ReplicationClient extends ByteChannelTransport {
	public ReplicationClient(ByteChannel channel, long lsn) {
		super(channel);
		ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(lsn);
		buffer.flip();
		while (buffer.hasRemaining()) {
			try {
				channel.write(buffer);
			} catch (IOException e) {
				throw new CommunicationException("Can't connect to tarantool", e);
			}
		}
		try {
			ByteBuffer version = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
			while (version.remaining() > 0 && channel.read(version) > 0) {
			}
			version.flip();
			int v = version.getInt();
			if (v != Const.VERSION) {
				throw new CommunicationException("Server version " + v + " is not supported");
			}
		} catch (IOException e) {
			throw new CommunicationException("Can't get version", e);
		}
	}
	
	public Tuple readTuple() {
		return read().readSingleTuple();
	}
}
