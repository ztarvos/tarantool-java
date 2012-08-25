package org.tarantool.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.text.ParseException;

import org.junit.Test;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.facade.Mapping;
import org.tarantool.facade.TarantoolTemplate;
import org.tarantool.facade.User;
import org.tarantool.pool.SocketChannelPooledConnectionFactory;

public class TestTemplate {
	public static final int TEMPLATE_SPACE = 125;

	@Test
	public void testCycle() throws ParseException, MalformedURLException {
		User user = new User();
		SocketChannelPooledConnectionFactory connectionFactory = new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10);
		TarantoolTemplate<User> template = new TarantoolTemplate<User>(TEMPLATE_SPACE, connectionFactory, new Mapping<User>(User.class, "id", "phone", "point",
				"iq", "height", "lifeFormId", "salary", "birthday", "name", "sign", "male"));
		assertNotNull(template.save(user).insertOrReplaceAndGet());
		try {
			template.save(user).insert();
			fail();
		} catch (TarantoolException ignored) {

		}
		assertEquals(1, template.save(user).replace());
		assertNotNull(template.find(0, "id").condition(user.getId()).list());
		assertEquals(user.getPhone() + 1L, template.update(user.getId()).add("phone", 1).updateAndGet().getPhone());

		connectionFactory.free();
		return;
	}

	@Test
	public void testCycle2() throws ParseException, MalformedURLException {
		User user = new User();
		SocketChannelPooledConnectionFactory connectionFactory = new SocketChannelPooledConnectionFactory("localhost", 33313, 1, 10);
		TarantoolTemplate<User> template = new TarantoolTemplate<User>(TEMPLATE_SPACE, connectionFactory, new Mapping<User>(User.class));
		assertNotNull(template.save(user).insertOrReplaceAndGet());
		try {
			template.save(user).insert();
			fail();
		} catch (TarantoolException ignored) {

		}
		assertEquals(1, template.save(user).replace());
		assertNotNull(template.find(0, "id").condition(user.getId()).list());
		assertNotNull(template.find(1).condition(user.getName()).one());
		assertEquals(user.getPhone() + 1L, template.update(user.getId()).add("phone", 1).updateAndGet().getPhone());

		connectionFactory.free();
		return;
	}
}
