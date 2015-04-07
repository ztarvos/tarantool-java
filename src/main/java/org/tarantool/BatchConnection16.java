package org.tarantool;

public interface BatchConnection16 extends TarantoolConnection16 {
    void begin();

    void end();

    void get();
}
