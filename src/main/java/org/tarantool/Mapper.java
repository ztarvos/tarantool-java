package org.tarantool;

import java.util.List;

public interface Mapper {
    Object toTuple(Object generic);

    Object[] toTuples(Object[] generic);

    <T> T toObject(Class<T> cls, List list);
}
