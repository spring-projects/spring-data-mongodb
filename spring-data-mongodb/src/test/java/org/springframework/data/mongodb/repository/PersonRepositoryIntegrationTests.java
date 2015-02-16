/*
 * Copyright 2010-2015 the original author or authors.
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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.util.AutoCloseableIterator;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	@Test
	public void findsAllMusiciansWithCursor() throws Exception {

		repository.save(new Person("foo", "bar"));
		repository.save(new Person("bar", "bar"));
		repository.save(new Person("fuu", "bar"));
		repository.save(new Person("notfound", "bar"));

		List<String> firstNames = Arrays.asList("bar", "foo", "fuu");
		AutoCloseableIterator<Person> result = repository.findByCustomQueryWithCursorByFirstnames(firstNames);

		try {
			for (Person person : result) {
				System.out.printf("%s%n", person);
			}
		} finally {
			result.close();
		}
	}
}
