package org.tarantool.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Const.OP;

public class Response {
	protected OP op;
	protected int size;
	protected int id;
	protected int ret;
	protected byte[] body;
	protected int count;

	public Response(int op, int size, int id) {
		super();
		this.op = OP.valueOf(op);
		this.size = size;
		this.id = id;
	}

	public OP getOp() {
		return op;
	}

	public void setOp(OP op) {
		this.op = op;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getRet() {
		return ret;
	}

	public void setRet(int ret) {
		this.ret = ret;
	}

	public byte[] getSrc() {
		return body;
	}

	public void setSrc(byte[] src) {
		this.body = src;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public List<Tuple> readTuples() {
		ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
		int count = buffer.getInt();
		List<Tuple> tuples = new ArrayList<Tuple>(count);
		for (int j = 0; j < count; j++) {
			tuples.add(Tuple.createFQ(buffer, ByteOrder.LITTLE_ENDIAN));
		}
		return tuples;
	}

	public Tuple readSingleTuple() {
		List<Tuple> tuples = readTuples();
		return tuples == null || tuples.isEmpty() ? null : tuples.get(0);
	}

}
