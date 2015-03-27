package org.tarantool;


import java.util.List;

public interface TarantoolConnection16 {

    List select(int space, int index, Object key, int offset, int limit, int iterator);


    List insert(int space, Object tuple);


    List replace(int space, Object tuple);


    List update(int space, Object key, Object tuple);


    List delete(int space, Object key);


    List call(String function, Object... args);

    List eval(String expression, Object... args);

    void auth(String username, String password);

    boolean ping();

    void close();

}
