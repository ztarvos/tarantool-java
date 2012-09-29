package org.tarantool.facade.chain;

public interface OperationFirst<T> {
	Update<T> add(String name, long value);

	Update<T> max(String name, long value);

	Update<T> sub(String name, long value);

	Update<T> add(String name, int value);

	Update<T> and(String name, int value);

	Update<T> and(String name, long value);

	Update<T> or(String name, int value);

	Update<T> or(String name, long value);

	Update<T> xor(String name, int value);

	Update<T> xor(String name, long value);

	Update<T> delete(String name);

	Update<T> insert(String name, Object value);

	Update<T> set(String name, Object value);

	Update<T> splice(String name, int offset, int delete, byte[] insert);

	Update<T> splice(String name, String value, int offset, int delete, String insert);
}
