package org.tarantool.core.cmd;

import java.io.Closeable;

public interface Transport extends Closeable {

	Response execute(Request request);

}