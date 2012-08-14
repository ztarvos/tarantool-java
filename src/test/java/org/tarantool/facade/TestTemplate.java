package org.tarantool.facade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.util.Date;

import org.junit.Test;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.SocketChannelConnectionFactory;

public class TestTemplate {
	@Test
	public void testCycle() throws MalformedURLException {
		Mapping<User> mapping = new Mapping<User>(User.class, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign",
				"male");
		TarantoolTemplate<User> tpl = new TarantoolTemplate<User>(125, new SocketChannelConnectionFactory("localhost", 33313, 1, 10), mapping);
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
		User user = new User();
		tpl.delete(user.getId()).delete();
		assertNull(tpl.find(0, "id").condition(123).one());

		assertEquals(1, tpl.save(user).insertOrReplace());
		try {
			tpl.save(user).insert();
			fail();
		} catch (TarantoolException e) {

		}
		user.setBirthday(new Date((user.getBirthday().getTime() / 1000L) * 1000L));
		assertTrue(user.equals(tpl.save(user).replaceAndGet()));

		assertNotNull(tpl.find(1, "name").condition(user.getName()).one());
	}
}
