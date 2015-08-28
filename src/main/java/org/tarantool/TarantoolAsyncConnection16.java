package org.tarantool;

import java.util.List;
import java.util.concurrent.Future;

public interface TarantoolAsyncConnection16 {
    Future<List> select(int space, int index, Object key, int offset, int limit, int iterator);

    Future<List> insert(int space, Object tuple);

    Future<List> replace(int space, Object tuple);

    Future<List> update(int space, Object key, Object... args);

    Future<List> delete(int space, Object key);

    Future<List> call(String function, Object... args);

    Future<List> eval(String expression, Object... args);

    boolean isValid();

    void close();
}
