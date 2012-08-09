package org.tarantool.core;

import java.io.Closeable;

public interface Transport extends Closeable{

	 Response execute(Request request);

}