package org.tarantool.msgpack;


import java.util.List;

import org.msgpack.type.Value;
import org.tarantool.pool.Connection;

public interface TarantoolConnection16 extends Connection {

    <R> R select(Class<R> cls, int space, int index, Object key, int offset, int limit);

    List<Value> select(int space, int index, Object key, int offset, int limit);

    <R> R insert(Class<R> cls, int space, Object tuple);

    List<Value> insert(int space, Object tuple);

    List<Value> replace(int space, Object tuple);

    <R> R replace(Class<R> cls, int space, Object tuple);

    List<Value> update(int space, Object key, Object tuple);

    <R> R update(Class<R> cls, int space, Object key, Object tuple);

    List<Value> delete(int space, Object key);

    <R> R delete(Class<R> cls, int space, Object key);

    List<Value> call(String function, Object ... args);

    <R> R call(Class<R> cls, String function, Object ... args);

    void auth(String username, String password);

    Boolean ping();

    void close();

}
