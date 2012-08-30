package org.tarantool.facade;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.core.Tuple;

/**
 * Simplifies all serialization and deserialization operations
 */
public class TupleSupport {
	public static String DEFAULT_ENCODING = "UTF-8";
	private static final Map<Class<?>, Class<?>> PRIMITIVE_MAP_CONVERSION = new HashMap<Class<?>, Class<?>>();

	static {
		Class<?>[] prim = new Class[] { void.class, Void.class, boolean.class, Boolean.class, byte.class, Byte.class, char.class, Character.class, short.class,
				Short.class, int.class, Integer.class, float.class, Float.class, double.class, Double.class, long.class, Long.class };
		for (int i = 0; i < prim.length; i += 2) {
			PRIMITIVE_MAP_CONVERSION.put(prim[i], prim[i + 1]);
		}

	}
	/**
	 * Default encoding for string conversion operations
	 */
	protected String encoding = "UTF-8";
	/**
	 * List of supported classes
	 */
	protected List<Class<?>> supported = new ArrayList<Class<?>>(Arrays.<Class<?>> asList(Long.class, long.class, Integer.class, int.class, Short.class,
			short.class, Double.class, double.class, Float.class, float.class, BigDecimal.class, BigInteger.class, Date.class, String.class, byte[].class,
			Boolean.class, boolean.class));

	/**
	 * Checks is class convertable
	 * 
	 * @param cls
	 * @return
	 */
	public boolean isClassSupported(Class<?> cls) {
		if (supported.contains(cls))
			return true;
		for (Class<?> s : supported) {
			if (s.isAssignableFrom(cls)) {
				return true;
			}
		}
		return false;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * Creates new Tuple from array of given objects
	 * 
	 * @param args
	 * @return
	 */
	public Tuple create(Object... args) {

		Tuple tuple = new Tuple(args.length);
		for (int i = 0; i < args.length; i++) {
			Object object = args[i];
			if (object == null) {
				throw new NullPointerException("Null values are not suppored, but argument #" + i + " has null value");
			}
			ser(tuple, i, object);
		}
		return tuple;

	}

	/**
	 * Sets tuple element no i from given object
	 * 
	 * @param tuple
	 * @param i
	 * @param object
	 */
	protected void ser(Tuple tuple, int i, Object object) {
		Class<? extends Object> cls = getNonPrimClassOf(object);
		if (Boolean.class.isAssignableFrom(cls)) {
			serBoolean(tuple, i, object);
		} else if (Number.class.isAssignableFrom(cls)) {
			serNumber(tuple, i, (Number) object);
		} else if (String.class.isAssignableFrom(cls)) {
			serString(tuple, i, (String) object);
		} else if (byte[].class.isAssignableFrom(cls)) {
			tuple.setBytes(i, (byte[]) object);
		} else if (Date.class.isAssignableFrom(cls)) {
			serDate(tuple, i, object);
		} else {
			serUnknown(tuple, i, object);
		}
	}

	/**
	 * Gets non primitive class of given object
	 * 
	 * @param object
	 * @return
	 */
	protected Class<? extends Object> getNonPrimClassOf(Object object) {
		Class<? extends Object> cls = object.getClass();
		return getNonPrimClass(cls);
	}

	/**
	 * Converts given class to non primitive
	 * 
	 * @param cls
	 * @return
	 */
	protected Class<? extends Object> getNonPrimClass(Class<? extends Object> cls) {
		if (cls.isPrimitive()) {
			cls = PRIMITIVE_MAP_CONVERSION.get(cls);
		}
		return cls;
	}

	protected void serBoolean(Tuple tuple, int i, Object object) {
		tuple.setBoolean(i, (Boolean) object);
	}

	/**
	 * Deserializes tuple to array of objects with given type of each element.
	 * 
	 * @param tuple
	 * @param cls
	 * @return
	 */
	public Object[] parse(Tuple tuple, Class<?>... cls) {
		Object[] result = new Object[cls.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = parse(tuple, i, cls[i]);
		}
		return result;
	}

	/**
	 * Deserializes element with given position from tuple
	 * 
	 * @param tuple
	 * @param i
	 * @param cls
	 * @return
	 */
	protected Object parse(Tuple tuple, int i, Class<?> cls) {
		Class<? extends Object> c = getNonPrimClass(cls);
		if (Boolean.class.isAssignableFrom(c)) {
			return deserBoolean(tuple, i);
		} else if (Number.class.isAssignableFrom(c)) {
			return deserNumber(tuple, c, i);
		} else if (String.class.isAssignableFrom(c)) {
			return deserString(tuple, i);
		} else if (byte[].class.equals(c)) {
			return tuple.getBytes(i);
		} else if (Date.class.isAssignableFrom(c)) {
			return deserDate(tuple, i);
		} else {
			return deserUnknown(tuple, c, i);
		}
	}

	protected Object deserBoolean(Tuple tuple, int i) {
		return tuple.getBoolean(i);
	}

	/**
	 * Deserializes unknown type with basic java deserialization mechanism. This
	 * method should be overriden if you want to add new data types.
	 * 
	 * @param tuple
	 * @param cls
	 * @param i
	 * @return
	 */
	protected Object deserUnknown(Tuple tuple, Class<?> cls, int i) {
		ByteArrayInputStream bis = new ByteArrayInputStream(tuple.getBytes(i));
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(bis);
			Object obj = ois.readObject();
			if (cls.isAssignableFrom(obj.getClass())) {
				throw new IllegalArgumentException("Waiting for " + cls + " but got " + obj.getClass());
			}
			return obj;
		} catch (IOException e) {
			throw new IllegalArgumentException("Can't deserialize " + cls + " #" + i, e);
		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Can't deserialize " + cls + " #i", e);
		}
	}

	protected Object deserDate(Tuple tuple, int i) {
		return tuple.getDate(i);
	}

	protected Object deserString(Tuple tuple, int i) {
		return tuple.getString(i, encoding);
	}

	protected Object deserNumber(Tuple tuple, Class<?> cls, int i) {
		if (Integer.class.isAssignableFrom(cls)) {
			return tuple.getInt(i);
		} else if (Long.class.isAssignableFrom(cls)) {
			return tuple.getLong(i);
		} else if (Short.class.isAssignableFrom(cls)) {
			return tuple.getShort(i);
		} else if (BigInteger.class.isAssignableFrom(cls)) {
			return tuple.getBigInteger(i);
		} else if (BigDecimal.class.isAssignableFrom(cls)) {
			return tuple.getBigDecimal(i);
		} else if (Double.class.isAssignableFrom(cls)) {
			return tuple.getDouble(i);
		} else if (Float.class.isAssignableFrom(cls)) {
			return tuple.getFloat(i);
		} else {
			throw new IllegalArgumentException("Don't know how to deserialize " + cls);
		}
	}

	protected void serDate(Tuple tuple, int i, Object object) {
		tuple.setDate(i, (Date) object);
	}

	/**
	 * Serializes unknown type using basic java serialization mechanism. This
	 * method should be overriden if you want to add new data types.
	 * 
	 * @param tuple
	 * @param i
	 * @param object
	 */
	protected void serUnknown(Tuple tuple, int i, Object object) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bos);
			oos.writeObject(object);
			oos.flush();
			oos.close();
			tuple.setBytes(i, bos.toByteArray());
		} catch (IOException e) {
			throw new IllegalArgumentException("Can't serialize " + object.getClass(), e);
		}
	}

	protected void serNumber(Tuple tuple, int i, Number object) {
		Class<? extends Object> cls = getNonPrimClassOf(object);
		if (Integer.class.isAssignableFrom(cls)) {
			tuple.setInt(i, (Integer) object);
		} else if (Long.class.isAssignableFrom(cls)) {
			tuple.setLong(i, (Long) object);
		} else if (Short.class.isAssignableFrom(cls)) {
			tuple.setShort(i, (Short) object);
		} else if (BigInteger.class.isAssignableFrom(cls)) {
			tuple.setBigInteger(i, (BigInteger) object);
		} else if (BigDecimal.class.isAssignableFrom(cls)) {
			tuple.setBigDecimal(i, (BigDecimal) object);
		} else if (Double.class.isAssignableFrom(cls)) {
			tuple.setDouble(i, (Double) object);
		} else if (Float.class.isAssignableFrom(cls)) {
			tuple.setFloat(i, (Float) object);
		} else {
			throw new IllegalArgumentException("Don't know how to serialize " + object.getClass());
		}

	}

	protected Tuple serString(Tuple tuple, int i, String string) {
		return tuple.setString(i, string, encoding);
	}

	public String getEncoding() {
		return encoding;
	}

	public boolean isAssignable(Class<?> a, Class<?> b) {
		return getNonPrimClass(a).isAssignableFrom(getNonPrimClass(b));
	}
}
