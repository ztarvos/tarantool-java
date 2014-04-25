package org.msgpack.template;

public interface EnumWithId<T> {
    int getId();

    T getById(int id);
}
