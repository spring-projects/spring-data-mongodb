/*
 * Copyright 2014-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.index;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexResolver.CycleGuard.Path;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * Unit tests for {@link Path}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class PathUnitTests {

	@Mock MongoPersistentEntity<?> entityMock;

	@Before
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setUp() {
		when(entityMock.getType()).thenReturn((Class) Object.class);
	}

	@Test // DATAMONGO-962, DATAMONGO-1782
	public void shouldIdentifyCycle() {

		MongoPersistentProperty foo = createPersistentPropertyMock(entityMock, "foo");
		MongoPersistentProperty bar = createPersistentPropertyMock(entityMock, "bar");

		Path path = Path.of(foo).append(bar).append(bar);

		assertThat(path.isCycle()).isTrue();
		assertThat(path.toCyclePath()).isEqualTo("bar -> bar");
		assertThat(path.toString()).isEqualTo("foo -> bar -> bar");
	}

	@Test // DATAMONGO-1782
	public void isCycleShouldReturnFalseWhenNoCyclePresent() {

		MongoPersistentProperty foo = createPersistentPropertyMock(entityMock, "foo");
		MongoPersistentProperty bar = createPersistentPropertyMock(entityMock, "bar");

		Path path = Path.of(foo).append(bar);

		assertThat(path.isCycle()).isFalse();
		assertThat(path.toCyclePath()).isEqualTo("");
		assertThat(path.toString()).isEqualTo("foo -> bar");
	}

	@Test // DATAMONGO-1782
	public void isCycleShouldReturnFalseCycleForNonEqualProperties() {

		MongoPersistentProperty foo = createPersistentPropertyMock(entityMock, "foo");
		MongoPersistentProperty bar = createPersistentPropertyMock(entityMock, "bar");
		MongoPersistentProperty bar2 = createPersistentPropertyMock(mock(MongoPersistentEntity.class), "bar");

		assertThat(Path.of(foo).append(bar).append(bar2).isCycle()).isFalse();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static MongoPersistentProperty createPersistentPropertyMock(MongoPersistentEntity owner, String fieldname) {

		MongoPersistentProperty property = Mockito.mock(MongoPersistentProperty.class);

		when(property.getOwner()).thenReturn(owner);
		when(property.getName()).thenReturn(fieldname);

		return property;
	}
}
