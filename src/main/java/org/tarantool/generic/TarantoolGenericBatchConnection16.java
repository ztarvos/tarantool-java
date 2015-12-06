package org.tarantool.generic;


import org.tarantool.batch.TarantoolBatchConnection16;

/**
 * You should use NamedConnections instead
 */
@Deprecated
public interface TarantoolGenericBatchConnection16 extends TarantoolBatchConnection16 {
     interface Holder<T> {
       T get(); 
    }
    
    <T> Holder<T> select(Class<T> clz, int space, int index, Object key, int offset, int limit, int iterator);


    <T> Holder<T> insert(Class<T> clz, int space, Object tuple);


    <T> Holder<T> replace(Class<T> clz, int space, Object tuple);


    <T> Holder<T> update(Class<T> clz, int space, Object key, Object... args);


    <T> Holder<T> delete(Class<T> clz, int space, Object key);


    <T> Holder<T> call(Class<T> clz, String function, Object... args);

    <T> Holder<T> eval(Class<T> clz, String expression, Object... args);

}
