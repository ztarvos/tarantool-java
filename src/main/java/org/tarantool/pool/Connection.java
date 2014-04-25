package org.tarantool.pool;

import java.io.Closeable;

public interface Connection extends Closeable {
    Boolean ping();
}
