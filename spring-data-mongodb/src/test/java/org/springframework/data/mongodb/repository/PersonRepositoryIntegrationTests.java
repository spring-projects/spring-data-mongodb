/*
 * Copyright 2010-2012 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	/**
	 * @see DATAMONGO-348
	 */
	@Test
	public void shouldLoadAssociationWithDbRefAndLazyLoading() throws Exception {

		operations.remove(new org.springframework.data.mongodb.core.query.Query(), User.class);

		User thomas = new User();
		thomas.username = "Oliver";
		operations.save(thomas);

		Person person = new Person();
		person.setFirstname("Thomas");
		person.setFans(Arrays.asList(thomas));
		repository.save(person);

		Person oliver = repository.findOne(person.id);
		List<User> fans = oliver.getFans();
		// TODO test internal object state of 'fans' before accessing
		// initialized should be 'false'
		// result should be 'null'

		User user = fans.get(0);
		// TODO test internal object state of 'fans' after accessing
		// initialized should be 'true'
		// result should be not 'null'
		// other fields should be 'null'

		assertThat(user.username, is(thomas.username));
	}
}
