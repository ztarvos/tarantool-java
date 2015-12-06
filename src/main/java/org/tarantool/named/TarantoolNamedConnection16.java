package org.tarantool.named;


import java.util.List;
import java.util.Map;

import org.tarantool.TarantoolConnection16Ops;

public interface TarantoolNamedConnection16 extends TarantoolConnection16Ops<String,Map<String,Object>,UpdateOperation,List> {
}
