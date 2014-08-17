package org.msgpack.template;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class LessLockTemplateRegistry extends TemplateRegistry {
    protected static final Map<Type, Template> cache = new ConcurrentHashMap<>();
    protected CustomAnyTemplate anyTemplate;

    /**
     * @param registry
     */
    public LessLockTemplateRegistry(TemplateRegistry registry) {
        super(registry);
    }


    @Override
    protected void registerTemplatesWhichRefersRegistry() {
        AnyTemplate anyTemplate = getAnyTemplate();

        register(List.class, new ListTemplate(anyTemplate));
        register(Set.class, new SetTemplate(anyTemplate));
        register(Collection.class, new CollectionTemplate(anyTemplate));
        register(Map.class, new MapTemplate(anyTemplate, anyTemplate));
        registerGeneric(List.class, new GenericCollectionTemplate(this, ListTemplate.class));
        registerGeneric(Set.class, new GenericCollectionTemplate(this, SetTemplate.class));
        registerGeneric(Collection.class, new GenericCollectionTemplate(this, CollectionTemplate.class));
        registerGeneric(Map.class, new GenericMapTemplate(this, MapTemplate.class));
    }

    protected AnyTemplate getAnyTemplate() {
        return this.anyTemplate == null ? this.anyTemplate = new CustomAnyTemplate(this) : anyTemplate;
    }


    protected Template getFromCache(Type type) {
        Template template = cache.get(type);
        if (template == null) {
            Class<?>[] interfaces = ((Class) type).getInterfaces();
            if (interfaces != null) {
                for (Class<?> iface : interfaces) {
                    if ((template = cache.get(iface)) != null) {
                        return template;
                    }
                }
            }
        }
        if (template == null) {
            Class<?> cls = (Class<?>) type;
            for (Class<?> superClass = cls.getSuperclass(); superClass != Object.class && superClass != null; superClass = superClass.getSuperclass()) {
                if ((template = cache.get(superClass)) != null) {
                    return template;
                }
            }
        }
        return template;
    }

    public Template get(Type type) {
        Template template = getFromCache(type);
        if (template == null) {
            template = lookup(type);
            cache.put(type, template);
        }
        return template;
    }

}
