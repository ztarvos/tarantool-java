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
import org.tarantool.facade.annotation.Field;
import org.tarantool.facade.annotation.Index;

/**
 * Maps class to tarantool tuple in specified space
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Mapping<T> {

	protected final List<Accessor> accessors;

	protected Class<T> cls;

	/**
	 * serialization and deserialization helper
	 */
	protected TupleSupport support;

	/**
	 * Tarantool space
	 */
	protected int space;

	/**
	 * Stores data about field
	 * 
	 */
	protected class Accessor {
		String name;
		Method read;
		Method write;
		Field field;
		Class<?> type;
		int idx;

		protected Accessor(String name, Method read, Method write, Class<?> type, int idx) {
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

	/**
	 * <p>
	 * Creates new Mapping
	 * </p>
	 * 
	 * @param cls
	 *            a {@link java.lang.Class} object.
	 */
	public Mapping(Class<T> cls) {
		this(cls, space(cls), fields(cls));

	}

	/**
	 * Creates new Mapping
	 * 
	 * @param cls
	 * @param support
	 *            instance of {@link TupleSupport}
	 */
	public Mapping(Class<T> cls, TupleSupport support) {
		this(cls, space(cls), support, fields(cls));

	}

	/**
	 * Returns tarantool space num for this class
	 * 
	 * @param cls
	 * @return space from {@link org.tarantool.facade.annotation.Tuple}
	 *         annotation
	 */
	public static <T> int space(Class<T> cls) {
		org.tarantool.facade.annotation.Tuple annotation = cls.getAnnotation(org.tarantool.facade.annotation.Tuple.class);
		if (annotation == null) {
			throw new IllegalArgumentException("Class should be annotated with @Tuple annotation");
		}
		return annotation.space();
	}

	/**
	 * <p>
	 * Gets fields from annotations
	 * </p>
	 * 
	 * @param cls
	 *            a {@link java.lang.Class} object.
	 * @return an array of {@link java.lang.String} objects.
	 */
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

	/**
	 * <p>
	 * Creates new Mapping.
	 * </p>
	 * 
	 * @param cls
	 *            a {@link java.lang.Class} object.
	 * @param fields
	 *            a {@link java.lang.String} object.
	 */
	public Mapping(Class<T> cls, int space, String... fields) {
		this(cls, space, new TupleSupport(), fields);
	}

	/**
	 * Stores field names which make up the index
	 */
	protected Map<Integer, String[]> indexes;

	/**
	 * Sets fields for index
	 * 
	 * @return a {@link org.tarantool.facade.Mapping} object.
	 */
	public Mapping<T> index(int indexNo, String... fields) {
		indexes.put(indexNo, fields);
		return this;
	}

	/**
	 * <p>
	 * Creates Mapping.
	 * </p>
	 * 
	 */
	@SuppressWarnings("unchecked")
	public Mapping(String className, int space, String... fields) throws ClassNotFoundException {
		this((Class<T>) Class.forName(className), space, new TupleSupport(), fields);

	}

	/**
	 * Creates Mapping.
	 */
	public Mapping(Class<T> cls, int space, TupleSupport support, String... fields) {
		this.space = space;
		this.cls = cls;
		this.support = support;
		this.accessors = new ArrayList<Accessor>(fields.length);
		indexes = new ConcurrentHashMap<Integer, String[]>();
		Map<Integer, SortedMap<Integer, String>> prepareIndex = new HashMap<Integer, SortedMap<Integer, String>>();
		for (int i = 0; i < fields.length; i++) {
			Accessor accessor = createAccessor(cls, fields[i], i);
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

	/**
	 * Creates new Instance of given class. Should be overriden if custom
	 * construction logick required
	 * 
	 * @param cls
	 *            a {@link java.lang.Class} object.
	 * @return a T object.
	 */
	protected T newInstance(Class<T> cls) {
		try {
			return cls.newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException(cls + " has no default constructor, you should override newInstance method");
		}
	}

	/**
	 * Creates {@link Accessor} for specified field
	 * 
	 * @param cls
	 * @param field
	 * @param idx
	 * @return accessor for field with give name
	 */
	protected Accessor createAccessor(Class<T> cls, String field, int idx) {
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

	/**
	 * Converts mapped object to {@link Tuple}. Can be overriden if custom
	 * logick is required
	 * 
	 * @param object
	 * @return Tuple created from given object
	 */
	public Tuple toTuple(T object) {
		if (object == null) {
			throw new NullPointerException();
		}
		Object[] objs = new Object[accessors.size()];
		for (int i = 0; i < objs.length; i++) {
			try {
				objs[i] = getValue(object, i);
			} catch (Exception e) {
				throw new IllegalStateException("Can't read property " + accessors.get(i).name + " of " + object.getClass(), e);
			}
		}
		return support.create(objs);
	}

	/**
	 * Reads value from object mapped on element i. Reflection performance
	 * penalty can be avoided here
	 */
	protected Object getValue(T object, int i) throws IllegalAccessException, InvocationTargetException {
		return accessors.get(i).read.invoke(object);
	}

	/**
	 * Creates mapped object from {@link Tuple}
	 * 
	 * @param tuple
	 * @return Object created from given tuple
	 */
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

	/**
	 * Sets value to field mapped on element i. Reflection performance penalty
	 * can be avoided here
	 */
	protected void setValue(T newInstance, Object object, int i) throws IllegalAccessException, InvocationTargetException {
		accessors.get(i).write.invoke(newInstance, object);
	}

	/**
	 * Checks that current instance of {@link TupleSupport} can works with given
	 * class
	 */
	protected void checkSupport(String name, Class<?> cls, TupleSupport support) {
		if (!support.isClassSupported(cls)) {
			throw new IllegalArgumentException(cls + " is not supported by property " + name + " of this type, you should override ser method in TupleSupport");
		}
	}

	/**
	 * Checks that specified values has same type with given fields
	 */
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

	/**
	 * Gets array of index fields
	 * 
	 * @param idx
	 * @return list of fields
	 */
	public String[] indexFields(int idx) {
		return indexes.get(idx);
	}

	/**
	 * Gets field number by name
	 * 
	 * @param name
	 * @return position of field in tuple by field name
	 */
	public int getFieldNo(String name) {
		Accessor accessor = getAccessorByName(name);
		return accessor == null ? -1 : accessor.idx;
	}

	/**
	 * Gets field type by field name
	 * 
	 * @param name
	 * @return type of field
	 */
	public Class<?> getFieldType(String name) {
		Accessor accessor = getAccessorByName(name);
		return accessor == null ? null : accessor.type;
	}

	public int getSpace() {
		return space;
	}

	public void setSpace(int space) {
		this.space = space;
	}

	public Class<T> getMappedClass() {
		return cls;
	}

}
