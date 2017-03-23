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
package org.springframework.data.mongodb.repository.support;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import lombok.Value;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;

/**
 * Tests for {@link PersistableMongoEntityInformation}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 */
@RunWith(MockitoJUnitRunner.class)
public class PersistableMappingMongoEntityInformationUnitTests {

	@Mock MongoPersistentEntity<TypeImplementingPersistable> persistableImplementingEntityTypeInfo;

	@Before
	public void setUp() {
		when(persistableImplementingEntityTypeInfo.getType()).thenReturn(TypeImplementingPersistable.class);
	}

	@Test // DATAMONGO-1590
	public void considersPersistableIsNew() {

		PersistableMongoEntityInformation<TypeImplementingPersistable, Long> information = new PersistableMongoEntityInformation<TypeImplementingPersistable, Long>(
				new MappingMongoEntityInformation<TypeImplementingPersistable, Long>(persistableImplementingEntityTypeInfo));

		assertThat(information.isNew(new TypeImplementingPersistable(100L, false)), is(false));
	}

	@Value
	static class TypeImplementingPersistable implements Persistable<Long> {

		private static final long serialVersionUID = -1619090149320971099L;

		Long id;
		boolean isNew;
	}
}
