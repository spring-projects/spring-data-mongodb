/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	/**
	 * <strong>ATTENTION</strong>: <br/>
	 * Test requires {@literal com.mysema.querydsl:querydsl-mongodb:3.4.1} to run!<br />
	 * Run with: {@code mvn -Dquerydsl=3.4.1 clean install}. <br />
	 * <br />
	 * TODO: move this one to AbstractPersonRepositoryIntegrationTests.
	 * 
	 * @see DATAMONGO-972
	 */
	@Test
	public void shouldExecuteFindOnDbRefCorrectly() {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User user = new User();
		user.setUsername("Valerie Matthews");

		operations.save(user);

		dave.setCreator(user);
		operations.save(dave);

		assertThat(repository.findOne(QPerson.person.creator.eq(user)), is(dave));
	}
}
