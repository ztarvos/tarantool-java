package org.tarantool.async;

import java.util.List;
import java.util.concurrent.Future;

import org.tarantool.TarantoolConnection16Ops;

public interface TarantoolAsyncConnection16 extends TarantoolConnection16Ops<Integer,Object,Object,Future<List>>{
    boolean isValid();
}
