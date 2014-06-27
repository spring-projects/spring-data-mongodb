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

import org.hamcrest.core.Is;
import org.junit.Assert;
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
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class PathUnitTests {

	@SuppressWarnings("rawtypes")//
	private @Mock MongoPersistentEntity entityMock;

	@Before
	public void setUp() {
		Mockito.when(entityMock.getType()).thenReturn(Object.class);
	}

	/**
	 * @see DATAMONGO-962
	 */
	@Test
	public void shouldItentifyCycleForOwnerOfSameTypeAndMatchingPath() {

		MongoPersistentProperty property = createPersistentPropertyMock(entityMock, "foo");
		Assert.assertThat(new Path(property, "foo.bar").cycles(property, "foo.bar.bar"), Is.is(true));
	}

	/**
	 * @see DATAMONGO-962
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void shouldAllowMatchingPathForDifferentOwners() {

		MongoPersistentProperty existing = createPersistentPropertyMock(entityMock, "foo");

		MongoPersistentEntity entityOfDifferentType = Mockito.mock(MongoPersistentEntity.class);
		Mockito.when(entityOfDifferentType.getType()).thenReturn(String.class);
		MongoPersistentProperty toBeVerified = createPersistentPropertyMock(entityOfDifferentType, "foo");

		Assert.assertThat(new Path(existing, "foo.bar").cycles(toBeVerified, "foo.bar.bar"), Is.is(false));
	}

	/**
	 * @see DATAMONGO-962
	 */
	@Test
	public void shouldAllowEqaulPropertiesOnDifferentPaths() {

		MongoPersistentProperty property = createPersistentPropertyMock(entityMock, "foo");
		Assert.assertThat(new Path(property, "foo.bar").cycles(property, "foo2.bar.bar"), Is.is(false));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private MongoPersistentProperty createPersistentPropertyMock(MongoPersistentEntity owner, String fieldname) {

		MongoPersistentProperty property = Mockito.mock(MongoPersistentProperty.class);
		Mockito.when(property.getOwner()).thenReturn(owner);
		Mockito.when(property.getFieldName()).thenReturn(fieldname);
		return property;
	}
}
