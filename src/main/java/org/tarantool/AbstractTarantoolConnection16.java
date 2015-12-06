package org.tarantool;


public abstract class AbstractTarantoolConnection16<T,O,P,R> {

    public abstract R exec(Code code, Object... args);

    public R select(T space, T index, O key, int offset, int limit, int iterator) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.ITERATOR, iterator, Key.LIMIT, limit, Key.OFFSET, offset);
    }

    public R insert(T space, O tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public R replace(T space, O tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public R update(T space, O key, P... args) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, args);
    }

    public R upsert(T space, O key, O def, P... args) {
        return exec(Code.UPSERT, Key.SPACE, space, Key.KEY, key, Key.TUPLE, def, Key.UPSERT_OPS, args);
    }

    public R delete(T space, O key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    public R call(String function, Object... args) {
        return exec(Code.CALL, Key.FUNCTION, function, Key.TUPLE, args);
    }

    public R eval(String expression, Object... args) {
        return exec(Code.EVAL, Key.EXPRESSION, expression, Key.TUPLE, args);
    }


    public void ping() {
        exec(Code.PING);
    }

}
