package org.tarantool.facade;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.net.URL;

import junit.framework.Assert;

import org.junit.Test;
import org.tarantool.core.Tuple;

public class TestMapping {

	@Test
	public void testSuccessfullMapping() {
		new Mapping<User>(User.class, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign", "male");
	}

	@Test
	public void testUnsupportedType() {
		try {
			new Mapping<User>(User.class, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign", "male", "site");
			Assert.fail();
		} catch (IllegalArgumentException ignored) {

		}
	}

	@Test
	public void testCustomSer() throws MalformedURLException {
		Mapping<User> mapping = new Mapping<User>(User.class, new TupleSupport() {
			{
				supported.add(URL.class);
			}

			@Override
			protected void serUnknown(Tuple tuple, int i, Object object) {
				if (object instanceof URL) {
					tuple.setString(i, ((URL) object).toString(), DEFAULT_ENCODING);
				} else {
					super.serUnknown(tuple, i, object);
				}
			}

			@Override
			protected Object deserUnknown(Tuple tuple, Class<?> cls, int i) {
				if (URL.class.equals(cls)) {
					try {
						return new URL(tuple.getString(i, DEFAULT_ENCODING));
					} catch (MalformedURLException e) {
						throw new IllegalArgumentException(e);
					}
				} else {
					return super.deserUnknown(tuple, cls, i);
				}
			}
		}, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign", "male", "site");
		User user = new User();
		Tuple tuple = mapping.toTuple(user);
		User userCopy = mapping.fromTuple(tuple);
		assertArrayEquals(tuple.pack(), mapping.toTuple(userCopy).pack());
		assertEquals(user.getSite(), userCopy.getSite());
	}

	public static class Tester {
		int id;
		String name;

		@Field(value = 0, index = { @Index(fieldNo = 0, indexNo = 0) })
		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		@Field(value = 1, index = { @Index(fieldNo = 0, indexNo = 1) })
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

	@Test
	public void testAnnotationMapping() {
		Mapping<Tester> mapping = new Mapping<Tester>(Tester.class);
		assertTrue(mapping.getFieldType("name").equals(String.class));
		mapping.checkFields(mapping.indexFields(0), new Object[] { 1, "name" });
	}

}
