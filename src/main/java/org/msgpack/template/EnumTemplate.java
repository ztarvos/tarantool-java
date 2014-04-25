package org.msgpack.template;

import java.io.IOException;

import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

public class EnumTemplate<T extends EnumWithId<T>> extends AbstractTemplate<T> {
    private static final IntegerTemplate tpl = IntegerTemplate.getInstance();

    private T t;
    public EnumTemplate(T t) {
        this.t = t;
    }

    @Override
    public void write(Packer pk, T v, boolean required) throws IOException {
        tpl.write(pk, v.getId(), required);
    }

    @Override
    public T read(Unpacker u, T to, boolean required) throws IOException {
        Integer id = tpl.read(u, 0, required);
        return t.getById(id);
    }
}
