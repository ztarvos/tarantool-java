package org.tarantool.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.text.ParseException;

import org.junit.Test;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.core.impl.SocketChannelConnectionFactory;
import org.tarantool.facade.Mapping;
import org.tarantool.facade.TarantoolTemplate;
import org.tarantool.facade.User;

public class TestTemplate {

	@Test
	public void testCycle() throws ParseException, MalformedURLException {
		User user = new User();
		SocketChannelConnectionFactory connectionFactory = new SocketChannelConnectionFactory();
		TarantoolTemplate<User> template = new TarantoolTemplate<User>(0, connectionFactory, new Mapping<User>(User.class, "id", "phone", "point", "iq",
				"height", "lifeFormId", "salary", "birthday", "name", "sign", "male"));
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
}
