/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.mapping.event;

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import javax.validation.ConstraintViolationException;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link ValidatingMongoEventListener}.
 * 
 * @see DATAMONGO-36
 * @author Maciej Walkowiak
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ValidatingMongoEventListenerTests {

	public static @ClassRule MongoVersionRule version = MongoVersionRule.atLeast(new Version(2, 6));

	@Autowired MongoTemplate mongoTemplate;

	@Test
	public void shouldThrowConstraintViolationException() {

		User user = new User("john", 17);

		try {
			mongoTemplate.save(user);
			fail();
		} catch (ConstraintViolationException e) {
			assertThat(e.getConstraintViolations().size(), equalTo(2));
		}
	}

	@Test
	public void shouldNotThrowAnyExceptions() {
		mongoTemplate.save(new User("john smith", 18));
	}
}
