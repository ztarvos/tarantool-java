package org.tarantool.facade;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Implements Mapping for java 7
 */
public class Mapping7<T> extends Mapping<T> {
	protected static final Lookup lookup = MethodHandles.publicLookup();

	protected class Accessor7 extends Accessor {
		MethodHandle readHandle;
		MethodHandle writeHandle;

		protected Accessor7(String name, Method read, Method write, Class<?> type, int idx) {
			super(name, read, write, type, idx);
		}

	}

	public Mapping7(Class<T> cls, int space, String... fields) {
		super(cls, space, fields);
	}

	public Mapping7(Class<T> cls, int space, TupleSupport support, String... fields) {
		super(cls, space, support, fields);
	}

	public Mapping7(Class<T> cls, TupleSupport support) {
		super(cls, support);
	}

	public Mapping7(Class<T> cls) {
		super(cls);
	}

	public Mapping7(String className, int space, String... fields) throws ClassNotFoundException {
		super(className, space, fields);
	}

	@Override
	protected Accessor createAccessor(Class<T> cls, String field, int idx) {
		PropertyDescriptor pd;
		try {
			pd = new PropertyDescriptor(field, cls);
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException("Can't create accesor for property " + field + " of class " + cls, e);
		}
		checkSupport(field, pd.getPropertyType(), support);
		Accessor7 accessor = new Accessor7(field, pd.getReadMethod(), pd.getWriteMethod(), pd.getPropertyType(), idx);

		try {
			accessor.readHandle = lookup.unreflect(pd.getReadMethod());
			accessor.writeHandle = lookup.unreflect(pd.getWriteMethod());
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException("Can't find getter and setter for " + field + " of type " + cls, e);
		}
		return accessor;
	}

	@Override
	protected Object getValue(T object, int i) throws IllegalAccessException, InvocationTargetException {
		Accessor acc = accessors.get(i);
		if (acc instanceof Mapping7.Accessor7) {
			Accessor7 accessor = (Accessor7) acc;
			try {
				return accessor.readHandle.invoke(object);
			} catch (Throwable e) {
				throw new IllegalStateException("Can't read value for", e);
			}
		} else {
			return super.getValue(object, i);
		}
	}

	@Override
	protected void setValue(T newInstance, Object object, int i) throws IllegalAccessException, InvocationTargetException {
		Accessor acc = accessors.get(i);
		if (acc instanceof Mapping7.Accessor7) {
			Accessor7 accessor = (Accessor7) acc;
			try {
				accessor.writeHandle.invoke(newInstance, object);
			} catch (Throwable e) {
				throw new IllegalStateException("Can't read value for", e);
			}
		} else {
			super.setValue(newInstance, object, i);
		}
	}
}
