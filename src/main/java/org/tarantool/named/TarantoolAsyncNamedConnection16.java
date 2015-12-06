package org.tarantool.named;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.tarantool.TarantoolConnection16Ops;

public interface TarantoolAsyncNamedConnection16  extends TarantoolConnection16Ops<String,Map<String,Object>,UpdateOperation,Future<List>> {
    boolean isValid();
}
