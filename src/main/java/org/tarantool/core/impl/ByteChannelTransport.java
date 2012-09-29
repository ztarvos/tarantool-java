package org.tarantool.core.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ByteChannel;

import org.tarantool.core.cmd.Request;
import org.tarantool.core.cmd.Response;
import org.tarantool.core.cmd.Transport;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.exception.TarantoolException;

/**
 * <p>
 * ByteChannelTransport class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class ByteChannelTransport implements Transport {
	ByteChannel channel;
	static final int HEADER_SIZE = 12;

	/** {@inheritDoc} */
	@Override
	public synchronized Response execute(Request request) {
		write(request);
		return read();
	}

	/**
	 * <p>
	 * Constructor for ByteChannelTransport.
	 * </p>
	 * 
	 * @param channel
	 *            a {@link java.nio.channels.ByteChannel} object.
	 */
	public ByteChannelTransport(ByteChannel channel) {
		super();
		this.channel = channel;
	}

	/**
	 * <p>
	 * read.
	 * </p>
	 * 
	 * @return a {@link org.tarantool.core.cmd.Response} object.
	 */
	public Response read() {
		ByteBuffer headers = read(HEADER_SIZE);
		Response response = new Response(headers.getInt(), headers.getInt(), headers.getInt());
		if (response.getSize() > 0) {
			ByteBuffer body = read(response.getSize());
			response.setRet(body.getInt());
			if (response.getRet() != 0) {
				handleErrorMessage(response, body);
			}
			if (body.remaining() > 4) {
				byte[] answer = new byte[body.remaining()];
				body.get(answer);
				response.setBody(answer);
			} else {
				response.setCount(body.getInt());
			}
		}
		return response;
	}

	/**
	 * <p>
	 * handleErrorMessage.
	 * </p>
	 * 
	 * @param response
	 *            a {@link org.tarantool.core.cmd.Response} object.
	 * @param body
	 *            a {@link java.nio.ByteBuffer} object.
	 */
	protected void handleErrorMessage(Response response, ByteBuffer body) {
		byte[] message = new byte[body.capacity() - 4];
		body.get(message);
		throw new TarantoolException(response.getRet(), new String(message).trim());
	}

	/**
	 * <p>
	 * read.
	 * </p>
	 * 
	 * @param size
	 *            a int.
	 * @return a {@link java.nio.ByteBuffer} object.
	 */
	protected ByteBuffer read(int size) {
		ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
		int res = 0;
		try {
			while (buffer.hasRemaining() && (res = channel.read(buffer)) > -1) {
			}
		} catch (IOException e) {
			throw new CommunicationException("Can't read data", e);
		}
		if (res == -1) {
			throw new CommunicationException("Connection lost");
		}
		buffer.flip();
		return buffer;
	}

	/**
	 * <p>
	 * write.
	 * </p>
	 * 
	 * @param request
	 *            a {@link org.tarantool.core.cmd.Request} object.
	 */
	protected void write(Request request) {
		ByteBuffer recvBuffer = request.pack();
		while (recvBuffer.hasRemaining()) {
			try {
				channel.write(recvBuffer);
			} catch (IOException e) {
				throw new CommunicationException("Can't write packet to channel", e);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws IOException {
		if (channel.isOpen()) {
			channel.close();
		}
	}

}
