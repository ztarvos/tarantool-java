package org.tarantool.generic;


import org.tarantool.TarantoolConnection16;

public interface TarantoolGenericConnection16 extends TarantoolConnection16 {

    <T> T select(Class<T> clz, int space, int index, Object key, int offset, int limit, int iterator);


    <T> T insert(Class<T> clz, int space, Object tuple);


    <T> T replace(Class<T> clz, int space, Object tuple);


    <T> T update(Class<T> clz, int space, Object key, Object... args);


    <T> void upsert(Class<T> clz, int space, Object key, Object def, Object... args);


    <T> T delete(Class<T> clz, int space, Object key);


    <T> T call(Class<T> clz, String function, Object... args);

    <T> T eval(Class<T> clz, String expression, Object... args);

}
