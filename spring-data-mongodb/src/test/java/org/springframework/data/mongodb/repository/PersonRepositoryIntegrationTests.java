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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
	 * @see DATAMONGO-1165
	 */
	@Test
	public void shouldAllowReturningJava8StreamInCustomQuery() throws Exception {

		Stream<Person> result = repository.findByCustomQueryWithStreamingCursorByFirstnames(Arrays.asList("Dave"));

		try {
			List<Person> readPersons = result.collect(Collectors.<Person> toList());
			assertThat(readPersons, hasItems(dave));
		} finally {
			result.close();
		}
	}
}
