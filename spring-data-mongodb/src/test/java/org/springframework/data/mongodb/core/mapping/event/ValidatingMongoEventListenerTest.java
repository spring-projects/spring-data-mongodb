package org.springframework.data.mongodb.core.mapping.event;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.validation.ConstraintViolationException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ValidatingMongoEventListenerTest {
	@Autowired
	private MongoTemplate mongoTemplate;

	@Test
	public void shouldThrowConstraintViolationException() {
		//given
		User user = new User("john", 17);

		try {
			//when
			mongoTemplate.save(user);

			//then
			fail();
		} catch (ConstraintViolationException e) {
			assertThat(e.getConstraintViolations().size(), equalTo(2));
		}
	}

	@Test
	public void shouldNotThrowAnyExceptions() {
		//given
		User user = new User("john smith", 18);

		//when & then
		mongoTemplate.save(user);
	}
}
