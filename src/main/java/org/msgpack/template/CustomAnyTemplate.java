package org.msgpack.template;

import java.io.IOException;

import org.msgpack.MessageTypeException;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

public class CustomAnyTemplate extends AnyTemplate<Object> {
    protected LessLockTemplateRegistry registry;

    public CustomAnyTemplate(LessLockTemplateRegistry registry) {
        super(registry);
        this.registry = registry;
    }

    @Override
    public Object read(Unpacker u, Object to, boolean required) throws IOException, MessageTypeException {
        if(to == null) {
            return u.readValue();
        }
        return super.read(u, to, required);
    }

    @Override
    public void write(Packer pk, Object target, boolean required) throws IOException {
        if (target == null) {
            if (required) {
                throw new MessageTypeException("Attempted to write null");
            }
            pk.writeNil();
        } else {
            registry.get(target.getClass()).write(pk, target);

        }
    }


}
