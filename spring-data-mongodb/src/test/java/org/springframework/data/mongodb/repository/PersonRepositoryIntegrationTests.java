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

import java.util.List;

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findByExampleShouldResolveStuffCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");

		// needed to tweak stuff a bit since some field are automatically set - so we need to undo this
		ReflectionTestUtils.setField(sample, "id", null);
		ReflectionTestUtils.setField(sample, "createdAt", null);
		ReflectionTestUtils.setField(sample, "email", null);

		Page<Person> result = repository.findByExample(new Example<Person>(sample), new PageRequest(0, 10));
		Assert.assertThat(result.getNumberOfElements(), Is.is(2));
	}

	/**
	 * @see DATAMONGO-1245
	 */
	@Test
	public void findAllByExampleShouldResolveStuffCorrectly() {

		Person sample = new Person();
		sample.setLastname("Matthews");

		// needed to tweak stuff a bit since some field are automatically set - so we need to undo this
		ReflectionTestUtils.setField(sample, "id", null);
		ReflectionTestUtils.setField(sample, "createdAt", null);
		ReflectionTestUtils.setField(sample, "email", null);

		List<Person> result = repository.findAllByExample(new Example<Person>(sample));
		Assert.assertThat(result.size(), Is.is(2));
	}
}
