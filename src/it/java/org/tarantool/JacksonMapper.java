package org.tarantool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.generic.Mapper;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

public class JacksonMapper implements Mapper {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public Object toTuple(Object generic) {
        Map map = mapper.convertValue(generic, Map.class);
        List tuple = new ArrayList(map.values());
        return tuple;
    }

    @Override
    public Object[] toTuples(Object[] generic) {
        return mapper.convertValue(generic, Object[].class);
    }

    @Override
    public <T> T toObject(Class<T> cls, List list) {
        JavaType type = mapper.getTypeFactory().constructType(cls);
        SerializationConfig cfg = mapper.getSerializationConfig();
        BeanDescription description = type.isArrayType() ? cfg.introspect(type.getContentType()) : cfg.introspect(type);
        List<BeanPropertyDefinition> properties = description.findProperties();
        if (type.isArrayType()) {
            List result = new ArrayList(list.size());
            for (Object props : list) {
                if (props instanceof List) {
                    Map<String, Object> map = new HashMap<String, Object>(properties.size());
                    int i = 0;
                    for (BeanPropertyDefinition propery : properties) {
                        map.put(propery.getName(), ((List) props).get(i++));
                    }
                    result.add(map);
                } else {
                    result.add(props);
                }
            }
            return mapper.convertValue(result, cls);
        } else {
            Map<String, Object> map = new HashMap<String, Object>(list.size());
            int i = 0;
            for (BeanPropertyDefinition propery : properties) {
                map.put(propery.getName(), list.get(i++));
            }
            return mapper.convertValue(map, cls);
        }

    }
}
