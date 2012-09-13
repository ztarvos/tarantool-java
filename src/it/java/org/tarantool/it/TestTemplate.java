package org.tarantool.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.Date;

import org.junit.Test;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.facade.Mapping;
import org.tarantool.facade.Mapping7;
import org.tarantool.facade.TarantoolTemplate;
import org.tarantool.facade.TarantoolTemplate7;
import org.tarantool.facade.User;
import org.tarantool.pool.SocketChannelPooledConnectionFactory;

public class TestTemplate {
	public static final int TEMPLATE_SPACE = 125;
	public static final int TEMPLATE_CALL_SPACE = 127;

	@Test
	public void testCycle() throws ParseException, MalformedURLException {
		User user = new User();
		SocketChannelPooledConnectionFactory connectionFactory = new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10);
		Mapping<User> mapping = new Mapping<User>(User.class, TEMPLATE_SPACE, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday",
				"name", "sign", "male");

		TarantoolTemplate template = new TarantoolTemplate(connectionFactory);
		template.addMapping(mapping);
		assertNotNull(template.save(user).insertOrReplaceAndGet());
		try {
			template.save(user).insert();
			fail();
		} catch (TarantoolException ignored) {

		}
		assertEquals(1, template.save(user).replace());
		assertNotNull(template.find(User.class, 0, "id").condition(user.getId()).list());
		assertEquals(user.getPhone() + 1L, template.update(User.class, user.getId()).add("phone", 1).updateAndGet().getPhone());

		connectionFactory.free();
		return;
	}

	@Test
	public void testCycle2() throws ParseException, MalformedURLException {
		User user = new User();
		SocketChannelPooledConnectionFactory connectionFactory = new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10);
		TarantoolTemplate template = new TarantoolTemplate(connectionFactory);
		assertNotNull(template.save(user).insertOrReplaceAndGet());
		try {
			template.save(user).insert();
			fail();
		} catch (TarantoolException ignored) {

		}
		assertEquals(1, template.save(user).replace());
		assertNotNull(template.find(User.class, 0, "id").condition(user.getId()).list());
		assertNotNull(template.find(User.class, 1).condition(user.getName()).one());
		assertEquals(user.getPhone() + 1L, template.update(User.class, user.getId()).add("phone", 1).updateAndGet().getPhone());

		connectionFactory.free();
		return;
	}

	@Test
	public void testCycle3() throws MalformedURLException {
		Mapping<User> mapping = new Mapping7<User>(User.class, 125, "id", "phone", "point", "iq", "height", "lifeFormId", "salary", "birthday", "name", "sign",
				"male");
		TarantoolTemplate tpl = new TarantoolTemplate7(new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10));
		tpl.addMapping(mapping);
		try {
			tpl.find(User.class).condition("string");
			fail();
		} catch (IllegalArgumentException ignored) {

		}
		try {
			tpl.find(User.class, 1, "name", "phone").condition("string", 123);
			fail();
		} catch (IllegalArgumentException ignored) {

		}
		User user = new User();
		tpl.delete(User.class, user.getId()).delete();
		assertNull(tpl.find(User.class, 0, "id").condition(123).one());

		assertEquals(1, tpl.save(user).insertOrReplace());
		try {
			tpl.save(user).insert();
			fail();
		} catch (TarantoolException e) {

		}
		user.setBirthday(new Date((user.getBirthday().getTime() / 1000L) * 1000L));
		assertTrue(user.equals(tpl.save(user).replaceAndGet()));

		assertNotNull(tpl.find(User.class, 1, "name").condition(user.getName()).one());
	}

	@Test
	public void testCall() throws MalformedURLException {
		Mapping<User> mapping = new Mapping<User>(User.class, 126, "id", "phone");
		SocketChannelPooledConnectionFactory connectionFactory = new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10);
		TarantoolTemplate template = new TarantoolTemplate(connectionFactory);
		template.addMapping(mapping);
		template.call(User.class, "box.delete", "126", 4321).callForOne();
		assertNotNull(template.call(User.class, "box.insert", "126", 4321, 323323L).callForOne());
		assertNull(template.call(User.class, "box.select", 126, 0, 4321).luaMode(true).callForOne());
		template.call(User.class, "box.delete", "126", 4321).callForOne();
	}
}
