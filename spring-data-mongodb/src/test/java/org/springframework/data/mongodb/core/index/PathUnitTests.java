/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.CycleGuard.Path;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Unit tests for {@link Path}.
 * 
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class PathUnitTests {

	@Mock MongoPersistentEntity<?> entityMock;

	@Before
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setUp() {
		when(entityMock.getType()).thenReturn((Class) Object.class);
	}

	/**
	 * @see DATAMONGO-962
	 */
	@Test
	public void shouldIdentifyCycleForOwnerOfSameTypeAndMatchingPath() {

		MongoPersistentProperty property = createPersistentPropertyMock(entityMock, "foo");
		assertThat(new Path(property, "foo.bar").cycles(property, "foo.bar.bar"), is(true));
	}

	/**
	 * @see DATAMONGO-962
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void shouldAllowMatchingPathForDifferentOwners() {

		MongoPersistentProperty existing = createPersistentPropertyMock(entityMock, "foo");

		MongoPersistentEntity entityOfDifferentType = Mockito.mock(MongoPersistentEntity.class);
		when(entityOfDifferentType.getType()).thenReturn(String.class);
		MongoPersistentProperty toBeVerified = createPersistentPropertyMock(entityOfDifferentType, "foo");

		assertThat(new Path(existing, "foo.bar").cycles(toBeVerified, "foo.bar.bar"), is(false));
	}

	/**
	 * @see DATAMONGO-962
	 */
	@Test
	public void shouldAllowEqaulPropertiesOnDifferentPaths() {

		MongoPersistentProperty property = createPersistentPropertyMock(entityMock, "foo");
		assertThat(new Path(property, "foo.bar").cycles(property, "foo2.bar.bar"), is(false));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private MongoPersistentProperty createPersistentPropertyMock(MongoPersistentEntity owner, String fieldname) {

		MongoPersistentProperty property = Mockito.mock(MongoPersistentProperty.class);
		when(property.getOwner()).thenReturn(owner);
		when(property.getFieldName()).thenReturn(fieldname);
		return property;
	}
}
