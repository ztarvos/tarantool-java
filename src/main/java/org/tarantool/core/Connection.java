package org.tarantool.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.tarantool.core.exception.CommunicationException;

public class Connection {
	Input input;
	Output output;
	SocketChannel channel;
	boolean autoReturn = false;
	ConnectionFactory factory;
	
	public Connection() {
		this("localhost",33013);
	}

	public Connection(String host, int port) {
		try {
			this.channel = SocketChannel.open(new InetSocketAddress(host, port));
		} catch (IOException e) {
			throw new CommunicationException("Can't connect to " + host + ":" + port, e);
		}
		AtomicInteger id = new AtomicInteger();
		createInput(id);
		createOutput(id);
	}

	public void setFactory(ConnectionFactory factory) {
		this.factory = factory;
	}

	private void autoReturn() {
		if (autoReturn && factory != null) {
			autoReturn = false;
			factory.returnConnection(this);
		}

	}

	public void autoReturn(ConnectionFactory factory) {
		this.autoReturn = true;
		this.factory = factory;
	}

	protected void createInput(AtomicInteger id) {
		input = new Input(channel, id);
	}

	protected void createOutput(AtomicInteger id) {
		output = new Output(channel, id);
	}

	public SocketChannel getChannel() {
		return channel;
	}

	public Integer delete(int space, Tuple id) {
		try {
			output.delete(space, 0, id);
			return input.readCount();
		} finally {
			autoReturn();
		}
	}

	public Tuple deleteAndGet(int space, Tuple id) {
		try {
			output.delete(space, Const.RETURN_TUPLE, id);
			return input.readTuple();
		} finally {
			autoReturn();
		}
	}

	public Tuple updateAndGet(int space, Tuple id, List<Operation> ops) {
		try {
			byte[][] encodedOps = new byte[ops.size()][];
			for (int i = 0; i < encodedOps.length; i++) {
				encodedOps[i] = ops.get(i).pack();
			}
			output.update(space, Const.RETURN_TUPLE, id, encodedOps);
			return input.readTuple();
		} finally {
			autoReturn();
		}

	}

	public Integer update(int space,  Tuple id, List<Operation> ops) {
		try {
			output.update(space, 0, id, ops.toArray(new byte[ops.size()][]));
			return input.readCount();
		} finally {
			autoReturn();
		}

	}

	public Tuple insertAndGet(int space, Tuple tuple) {
		try {
			output.insert(space, Const.ADD_TUPLE | Const.RETURN_TUPLE, tuple);
			return input.readTuple();
		} finally {
			autoReturn();
		}

	}

	public Integer insert(int space, Tuple tuple) {
		try {
			output.insert(space, Const.ADD_TUPLE, tuple);
			return input.readCount();
		} finally {
			autoReturn();
		}

	}

	public Integer replace(int space, Tuple tuple) {
		try {
			output.insert(space, Const.REPLACE_TUPLE, tuple);
			return input.readCount();
		} finally {
			autoReturn();
		}

	}

	public Tuple replaceAndGet(int space, Tuple tuple) {
		try {
			output.insert(space, Const.REPLACE_TUPLE | Const.RETURN_TUPLE, tuple);
			return input.readTuple();
		} finally {
			autoReturn();
		}

	}

	public Tuple insertOrReplaceAndGet(int space, Tuple tuple) {
		try {
			output.insert(space, Const.RETURN_TUPLE, tuple);
			return input.readTuple();
		} finally {
			autoReturn();
		}

	}

	public Integer insertOrReplace(int space, Tuple tuple) {
		try {
			output.insert(space, 0, tuple);
			return input.readCount();
		} finally {
			autoReturn();
		}
	}

	public List<Tuple> find(int space, int index, int offset, int limit, Tuple... keys) {
		try {
			output.select(space, index, offset, limit, keys);
			return input.readTuples();
		} finally {
			autoReturn();
		}

	}

	public List<Tuple> find(int space, int index, int offset, int limit, Collection<Tuple> keys) {
		return find(space, index, offset, limit, keys.toArray(new Tuple[keys.size()]));
	}

	public Tuple findOne(int space, int index, int offset, int limit, Tuple... keys) {
		try {
			output.select(space, index, offset, limit, keys);
			return input.readTuple();
		} finally {
			autoReturn();
		}
	}

	public Tuple findOne(int space, int index, int offset, int limit, Collection<Tuple> keys) {
		return findOne(space, index, offset, limit, keys.toArray(new Tuple[keys.size()]));
	}

	public void close() {
		try {
			if (channel != null && channel.isOpen()) {
				channel.close();
			}
		} catch (IOException e) {
			throw new CommunicationException("Can't close connection", e);
		}
	}

	public Input getInput() {
		return input;
	}

	public void setInput(Input input) {
		this.input = input;
	}

	public Output getOutput() {
		return output;
	}

	public void setOutput(Output output) {
		this.output = output;
	}

	public void setChannel(SocketChannel channel) {
		this.channel = channel;
	}

	public Boolean ping() {
		try {
			output.ping();
			return input.ping();
		} finally {
			autoReturn();
		}

	}

}
