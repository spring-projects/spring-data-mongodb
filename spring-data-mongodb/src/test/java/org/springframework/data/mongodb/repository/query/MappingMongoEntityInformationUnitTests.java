/*
 * Copyright 2011-2017 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.repository.query;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;

/**
 * Unit tests for {@link MappingMongoEntityInformation}.
 * 
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingMongoEntityInformationUnitTests {

	@Mock MongoPersistentEntity<Person> info;

	@Before
	public void setUp() {

		when(info.getType()).thenReturn(Person.class);
		when(info.getCollection()).thenReturn("Person");
	}

	@Test // DATAMONGO-248
	public void usesEntityCollectionIfNoCustomOneGiven() {

		MongoEntityInformation<Person, Long> information = new MappingMongoEntityInformation<Person, Long>(info);
		assertThat(information.getCollectionName(), is("Person"));
	}

	@Test // DATAMONGO-248
	public void usesCustomCollectionIfGiven() {

		MongoEntityInformation<Person, Long> information = new MappingMongoEntityInformation<Person, Long>(info, "foobar");
		assertThat(information.getCollectionName(), is("foobar"));
	}
}
