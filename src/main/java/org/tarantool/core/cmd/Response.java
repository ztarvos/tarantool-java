package org.tarantool.core.cmd;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Tuple;

/**
 * Tarantool server response
 *
 * @author dgreen
 * @version $Id: $
 */
public class Response {
	protected int op;
	protected int size;
	protected int id;
	protected int ret;
	protected byte[] body;
	protected int count = -1;

	/**
	 * <p>Constructor for Response.</p>
	 *
	 * @param op a int.
	 * @param size a int.
	 * @param id a int.
	 */
	public Response(int op, int size, int id) {
		super();
		this.op = op;
		this.size = size;
		this.id = id;
	}

	/**
	 * <p>Getter for the field <code>op</code>.</p>
	 *
	 * @return a int.
	 */
	public int getOp() {
		return op;
	}

	/**
	 * <p>Setter for the field <code>op</code>.</p>
	 *
	 * @param op a int.
	 */
	public void setOp(int op) {
		this.op = op;
	}

	/**
	 * <p>Getter for the field <code>size</code>.</p>
	 *
	 * @return a int.
	 */
	public int getSize() {
		return size;
	}

	/**
	 * <p>Setter for the field <code>size</code>.</p>
	 *
	 * @param size a int.
	 */
	public void setSize(int size) {
		this.size = size;
	}

	/**
	 * <p>Getter for the field <code>id</code>.</p>
	 *
	 * @return a int.
	 */
	public int getId() {
		return id;
	}

	/**
	 * <p>Setter for the field <code>id</code>.</p>
	 *
	 * @param id a int.
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * <p>Getter for the field <code>ret</code>.</p>
	 *
	 * @return a int.
	 */
	public int getRet() {
		return ret;
	}

	/**
	 * <p>Setter for the field <code>ret</code>.</p>
	 *
	 * @param ret a int.
	 */
	public void setRet(int ret) {
		this.ret = ret;
	}

	/**
	 * <p>getSrc.</p>
	 *
	 * @return an array of byte.
	 */
	public byte[] getSrc() {
		return body;
	}

	/**
	 * <p>setSrc.</p>
	 *
	 * @param src an array of byte.
	 */
	public void setSrc(byte[] src) {
		this.body = src;
	}

	/**
	 * <p>Getter for the field <code>body</code>.</p>
	 *
	 * @return an array of byte.
	 */
	public byte[] getBody() {
		return body;
	}

	/**
	 * <p>Setter for the field <code>body</code>.</p>
	 *
	 * @param body an array of byte.
	 */
	public void setBody(byte[] body) {
		this.body = body;
	}

	/**
	 * <p>Getter for the field <code>count</code>.</p>
	 *
	 * @return a int.
	 */
	public int getCount() {
		return count;
	}

	/**
	 * <p>Setter for the field <code>count</code>.</p>
	 *
	 * @param count a int.
	 */
	public void setCount(int count) {
		this.count = count;
	}

	/**
	 * <p>readTuples.</p>
	 *
	 * @return a {@link java.util.List} object.
	 */
	public List<Tuple> readTuples() {
		if (body == null && count == 0) {
			return new ArrayList<Tuple>();
		}
		ByteBuffer buffer = ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN);
		int count = buffer.getInt();
		List<Tuple> tuples = new ArrayList<Tuple>(count);
		for (int j = 0; j < count; j++) {
			tuples.add(Tuple.createFQ(buffer, ByteOrder.LITTLE_ENDIAN));
		}
		return tuples;
	}

	/**
	 * <p>readSingleTuple.</p>
	 *
	 * @return a {@link org.tarantool.core.Tuple} object.
	 */
	public Tuple readSingleTuple() {
		List<Tuple> tuples = readTuples();
		return tuples == null || tuples.isEmpty() ? null : tuples.get(0);
	}

}
