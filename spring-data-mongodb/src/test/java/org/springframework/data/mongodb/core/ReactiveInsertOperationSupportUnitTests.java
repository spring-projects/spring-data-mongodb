/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyList;

import lombok.Data;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;

/**
 * Unit tests for {@link ExecutableInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class ReactiveInsertOperationSupportUnitTests {

	private static final String STAR_WARS = "star-wars";

	@Mock ReactiveMongoTemplate template;

	ReactiveInsertOperationSupport ops;

	Person luke, han;

	@Before
	public void setUp() {

		when(template.determineCollectionName(any(Class.class))).thenReturn(STAR_WARS);

		ops = new ReactiveInsertOperationSupport(template);

		luke = new Person();
		luke.id = "id-1";
		luke.firstname = "luke";

		han = new Person();
		han.firstname = "han";
		han.id = "id-2";
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void nullCollectionShouldThrowException() {
		ops.insert(Person.class).inCollection(null);
	}

	@Test // DATAMONGO-1719
	public void insertShouldUseDerivedCollectionName() {

		ops.insert(Person.class).one(luke);

		ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);

		verify(template).determineCollectionName(captor.capture());
		verify(template).insert(eq(luke), eq(STAR_WARS));

		assertThat(captor.getAllValues()).containsExactly(Person.class);
	}

	@Test // DATAMONGO-1719
	public void insertShouldUseExplicitCollectionName() {

		ops.insert(Person.class).inCollection(STAR_WARS).one(luke);

		verify(template, never()).determineCollectionName(any(Class.class));
		verify(template).insert(eq(luke), eq(STAR_WARS));
	}

	@Test // DATAMONGO-1719
	public void insertCollectionShouldDelegateCorrectly() {

		ops.insert(Person.class).all(Arrays.asList(luke, han));

		verify(template).determineCollectionName(any(Class.class));
		verify(template).insert(anyList(), eq(STAR_WARS));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person {
		@Id String id;
		String firstname;
	}
}
