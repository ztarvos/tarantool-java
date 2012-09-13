package org.tarantool.core.cmd;

import java.nio.ByteBuffer;

import org.tarantool.core.Tuple;
import org.tarantool.core.proto.Leb128;

public class Call extends Request {

	public static final int OP_CODE = 22;

	int flags;
	String procName;
	byte[] body;

	public Call(int id, String procName, byte[] body) {
		super(OP_CODE, id);
		this.body = body;
		this.procName = procName;

	}

	public Call(int id, String procName, Tuple tuple) {
		this(id, procName, tuple.pack());
	}

	public Call flags(int flags) {
		this.flags = flags;
		return this;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	public String getProcName() {
		return procName;
	}

	public void setProcName(String procName) {
		this.procName = procName;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	@Override
	protected int getCapacity() {
		int procNameLen = procName.getBytes().length;
		return body.length + 4 + Leb128.unsignedSize(procNameLen) + procNameLen;
	}

	@Override
	public ByteBuffer body(ByteBuffer buffer) {
		byte[] procNameBytes = procName.getBytes();
		return Leb128.writeUnsigned(buffer.putInt(flags), procNameBytes.length).put(procNameBytes).put(body);
	}

}
