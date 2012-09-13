package org.tarantool.facade.chain;

import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Tuple;
import org.tarantool.facade.Mapping;
import org.tarantool.pool.SingleQueryConnectionFactory;

public class Call<T> extends Chain<T> {

	String procName;
	Object[] args;
	int flags;
	boolean luaMode = false;

	public Call(SingleQueryConnectionFactory factory, Mapping<T> mapping, String procName, Object... args) {
		super(factory, mapping);
		this.procName = procName;
		this.args = args;
	}

	public Call<T> flags(int flags) {
		this.flags = flags;
		return this;
	}

	public Call<T> luaMode(boolean value) {
		this.luaMode = value;
		return this;
	}

	public List<T> call() {

		Tuple params = luaMode ? luaArgs() : mapping.getSupport().create(args);
		List<Tuple> response = factory.getSingleQueryConnection().call(flags, procName, params);
		List<T> result = new ArrayList<T>();
		for (Tuple tuple : response) {
			result.add(mapping.fromTuple(tuple));
		}
		return result;
	}

	protected Tuple luaArgs() {
		List<Object> convertedArgs = new ArrayList<Object>(args.length);
		for (Object arg : args) {
			if (arg instanceof String) {
				String str = (String) arg;
				StringBuilder builder = new StringBuilder(str.length() + 2);
				builder.append("'");
				builder.append(str.replaceAll("\\", "\\\\").replaceAll("'", "\\'"));
				convertedArgs.add(builder.toString());
			} else {
				convertedArgs.add(String.valueOf(arg));
			}
		}
		return mapping.getSupport().create(convertedArgs.toArray(new Object[convertedArgs.size()]));
	}

	public T callForOne() {
		List<T> call = call();
		return call == null || call.isEmpty() ? null : call.get(0);
	}

}
