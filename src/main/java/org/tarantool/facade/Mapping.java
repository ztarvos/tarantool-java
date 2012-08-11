package org.tarantool.facade;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.tarantool.core.Tuple;

public class Mapping<T> {
	final List<Accessor> accessors;

	Class<T> cls;

	TupleSupport support;

	protected class Accessor {
		String name;
		Method read;
		Method write;
		Class<?> type;
		int idx;

		private Accessor(String name, Method read, Method write, Class<?> type, int idx) {
			super();
			this.name = name;
			this.read = read;
			this.write = write;
			this.type = type;
			this.idx = idx;
		}

	}

	public Mapping(Class<T> cls, String... fields) {
		this(cls, new TupleSupport(), fields);
		pk = new String[] { fields[0] };
	}

	String[] pk;

	public Mapping<T> primaryKey(String... fields) {
		pk = fields;
		return this;
	}

	@SuppressWarnings("unchecked")
	public Mapping(String className, String... fields) throws ClassNotFoundException {
		this((Class<T>) Class.forName(className), new TupleSupport(), fields);

	}

	public Mapping(Class<T> cls, TupleSupport support, String... fields) {
		this.cls = cls;
		this.support = support;
		this.accessors = new ArrayList<Accessor>(fields.length);
		for (int i = 0; i < fields.length; i++) {
			this.accessors.add(getAccessor(cls, fields[i], i));
		}

		newInstance(cls);

	}

	protected T newInstance(Class<T> cls) {
		try {
			return cls.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException(cls + " has no default constructor, you should override newInstance method");
		}
	}

	protected Accessor getAccessor(Class<T> cls, String field, int idx) {
		PropertyDescriptor pd;
		try {
			pd = new PropertyDescriptor(field, cls);
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException("Can't create accesor for property " + field + " of class " + cls, e);
		}
		checkSupport(field, pd.getPropertyType(), support);
		Accessor accessor = new Accessor(field, pd.getReadMethod(), pd.getWriteMethod(), pd.getPropertyType(), idx);
		return accessor;
	}

	public Tuple toTuple(T object) {
		if (object == null) {
			throw new NullPointerException();
		}
		Object[] objs = new Object[accessors.size()];
		for (int i = 0; i < objs.length; i++) {
			try {
				objs[i] = getValue(object, i);
			} catch (Exception e) {
				throw new IllegalStateException("Can't read property " + accessors.get(i) + " of " + object.getClass(), e);
			}
		}
		return support.create(objs);
	}

	protected Object getValue(T object, int i) throws IllegalAccessException, InvocationTargetException {
		return accessors.get(i).read.invoke(object);
	}

	public T fromTuple(Tuple tuple) {
		if (tuple == null) {
			return null;
		}
		if (tuple.size() != accessors.size()) {
			throw new IllegalArgumentException("Tuple can't be deserialized to " + cls + " cause tuple hasn't required amount of values. Should has "
					+ accessors.size() + " but has " + tuple.size());
		}

		Class<?>[] classes = new Class<?>[accessors.size()];
		for (int i = 0; i < accessors.size(); i++) {
			classes[i] = accessors.get(i).type;
		}
		Object[] objects = support.parse(tuple, classes);
		T newInstance = newInstance(cls);
		for (int i = 0; i < objects.length; i++) {
			try {
				setValue(newInstance, objects[i], i);
			} catch (Exception e) {
				throw new IllegalStateException("Can't set value for property #" + i + " for class " + cls + " " + accessors.get(i).name, e);
			}
		}
		return newInstance;

	}

	protected void setValue(T newInstance, Object object, int i) throws IllegalAccessException, InvocationTargetException {
		accessors.get(i).write.invoke(newInstance, object);
	}

	protected void checkSupport(String name, Class<?> cls, TupleSupport support) {
		if (!support.isClassSupported(cls)) {
			throw new IllegalArgumentException(cls + " is not supported by property " + name + " of this type, you should override ser method in TupleSupport");
		}
	}

	public void checkFields(String[] indexFields, Object[] values) {
		for (int i = 0; i < indexFields.length; i++) {
			Object value = values[i];
			String field = indexFields[i];
			Class<? extends Object> valueType = value.getClass();
			checkSupport(field, valueType, support);
			Accessor accessor = getAccessorByName(field);
			if (accessor == null) {
				throw new IllegalArgumentException("Value for " + field + ": " + value + " not found in field list");
			} else {
				if (!support.isAssignable(accessor.type, valueType)) {
					throw new IllegalArgumentException("Value for " + field + ": " + value + " has invalid type, should be " + accessor.type + " but has "
							+ valueType);
				}
			}
		}

	}

	private Accessor getAccessorByName(String field) {
		Accessor accessor = null;
		for (Accessor a : accessors) {
			if (a.name.equals(field)) {
				accessor = a;
			}
		}
		return accessor;
	}

	public TupleSupport getSupport() {
		return support;
	}

	public String[] getPrimaryKeyName() {
		return pk;
	}

	public int getFieldNo(String name) {
		Accessor accessor = getAccessorByName(name);
		return accessor == null ? -1 : accessor.idx;
	}

	public Class<?> getFieldType(String name) {
		Accessor accessor = getAccessorByName(name);
		return accessor == null ? null : accessor.type;
	}

}