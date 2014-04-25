package org.msgpack.template;

import java.lang.reflect.Type;

import org.msgpack.MessagePack;

public class LessLockMessagePack extends MessagePack {
    protected static final LessLockTemplateRegistry registry = new LessLockTemplateRegistry(null);


    public LessLockMessagePack() {
        super(registry);
    }

    @Override
    public <T> Template<T> lookup(Class<T> type) {
        return registry.get(type);
    }

    @Override
    public Template<?> lookup(Type type) {
        return registry.get(type);
    }

    public static LessLockTemplateRegistry getRegistry() {
        return registry;
    }
}
