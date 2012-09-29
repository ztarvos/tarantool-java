package org.tarantool.facade.chain;

public interface ContidionFirst<T> {

	Search<T> condition(Object... values);
}