package org.tarantool;

public interface TarantoolBatchConnection16 extends TarantoolConnection16 {
    void begin();

    void end();

    void get();
}
