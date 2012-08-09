package org.tarantool.facade;

import static org.junit.Assert.fail;

import org.junit.Test;

public class TestTemplate {
	@Test
	public void testTypeCheck() {
		Mapping<User> mapping = new Mapping<User>(User.class, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign",
				"male");
		TarantoolTemplate<User, Integer> tpl = new TarantoolTemplate<User, Integer>(0, null, mapping);
		try {
			tpl.find().condition("string");
			fail();
		} catch (IllegalArgumentException ignored) {

		}

		try {
			tpl.find(1, "name", "phone").condition("string", 123);
			fail();
		} catch (IllegalArgumentException ignored) {

		}

		tpl.find(1, "name", "phone").condition("string", 123L);

	}
}
