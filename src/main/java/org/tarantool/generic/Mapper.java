package org.tarantool.generic;

import java.util.List;

/**
 * You should use NamedConnections instead
 */
@Deprecated
public interface Mapper {
    Object toTuple(Object generic);

    Object[] toTuples(Object[] generic);

    <T> T toObject(Class<T> cls, List list);
}
