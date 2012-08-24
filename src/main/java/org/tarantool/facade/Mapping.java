package org.tarantool.facade;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.tarantool.core.Tuple;

public class Mapping<T> {
	final List<Accessor> accessors;

	Class<T> cls;

	TupleSupport support;

	protected class Accessor {
		String name;
		Method read;
		Method write;
		Field field;
		Class<?> type;
		int idx;

		private Accessor(String name, Method read, Method write, Class<?> type, int idx) {
			super();
			this.name = name;
			this.read = read;
			this.write = write;
			this.type = type;
			this.idx = idx;
			field = read.getAnnotation(Field.class);
			if (field == null) {
				field = write.getAnnotation(Field.class);
			}
		}

	}

	public Mapping(Class<T> cls) {
		this(cls, fields(cls));

	}

	public static String[] fields(Class<?> cls) {
		BeanInfo beanInfo;
		try {
			beanInfo = Introspector.getBeanInfo(cls);
			PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
			Map<Integer, String> order = new HashMap<Integer, String>();
			int max = 0;
			for (PropertyDescriptor prop : descriptors) {
				Field read = prop.getReadMethod().getAnnotation(Field.class);
				Field write = prop.getReadMethod().getAnnotation(Field.class);
				if (read != null || write != null) {
					int fieldNo = read == null ? write.value() : read.value();
					if (order.put(fieldNo, prop.getName()) != null) {
						throw new IllegalArgumentException(fieldNo + " used more than once in " + cls);
					}
					if (fieldNo < 0) {
						throw new IllegalArgumentException(fieldNo + " should be non negative in " + cls);
					}
					max = Math.max(max, fieldNo);
				}
			}
			String[] fields = new String[max + 1];
			for (int i = 0; i <= max; i++) {
				if ((fields[i] = order.get(i)) == null) {
					throw new IllegalArgumentException("fieldNo " + i + " not found in " + cls);
				}
			}
			return fields;
		} catch (IntrospectionException e) {
			throw new IllegalArgumentException("Can't get properties", e);
		}
	}

	public Mapping(Class<T> cls, String... fields) {
		this(cls, new TupleSupport(), fields);
	}

	Map<Integer, String[]> indexes;

	public Mapping<T> index(int no, String... fields) {
		indexes.put(no, fields);
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
		indexes = new ConcurrentHashMap<Integer, String[]>();
		Map<Integer, SortedMap<Integer, String>> prepareIndex = new HashMap<Integer, SortedMap<Integer, String>>();
		for (int i = 0; i < fields.length; i++) {
			Accessor accessor = getAccessor(cls, fields[i], i);
			this.accessors.add(accessor);
			if (accessor.field != null && accessor.field.index() != null && accessor.field.index().length > 0) {
				for (Index index : accessor.field.index()) {
					SortedMap<Integer, String> indexFields = prepareIndex.get(index.indexNo());
					if (indexFields == null)
						prepareIndex.put(index.indexNo(), indexFields = new TreeMap<Integer, String>());
					indexFields.put(index.fieldNo(), fields[i]);
				}
			}
		}
		for (Map.Entry<Integer, SortedMap<Integer, String>> entry : prepareIndex.entrySet()) {
			int max = Collections.max(entry.getValue().keySet());
			int min = Collections.min(entry.getValue().keySet());
			if (min == 0 && max - min == (entry.getValue().size() - 1)) {
				String[] indexFields = new String[entry.getValue().size()];
				for (int i = 0; i <= max; i++) {
					indexFields[i] = entry.getValue().get(i);
				}
				index(entry.getKey(), indexFields);
			} else {
				throw new IllegalArgumentException("Index No " + entry.getKey() + " fields has incorrect order");
			}
		}
		if (!indexes.containsKey(0)) {
			index(0, fields[0]);
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

	public String[] indexFields(int idx) {
		return indexes.get(idx);
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