/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.Person;
import org.springframework.data.mongodb.repository.support.MappingMongoEntityInformation;
import org.springframework.data.repository.core.EntityInformation;

/**
 * Unit tests for {@link MappingMongoEntityInformation}.
 *
 * @author Oliver Gierke
 */
@ExtendWith(MockitoExtension.class)
public class MappingMongoEntityInformationUnitTests {

	@Mock MongoPersistentEntity<Person> info;
	@Mock MongoPersistentEntity<TypeImplementingPersistable> persistableImplementingEntityTypeInfo;

	@Test // DATAMONGO-248
	public void usesEntityCollectionIfNoCustomOneGiven() {

		when(info.getCollection()).thenReturn("Person");

		MongoEntityInformation<Person, Long> information = new MappingMongoEntityInformation<Person, Long>(info);
		assertThat(information.getCollectionName()).isEqualTo("Person");
	}

	@Test // DATAMONGO-248
	public void usesCustomCollectionIfGiven() {

		MongoEntityInformation<Person, Long> information = new MappingMongoEntityInformation<Person, Long>(info, "foobar");
		assertThat(information.getCollectionName()).isEqualTo("foobar");
	}

	@Test // DATAMONGO-1590
	public void considersPersistableIsNew() {

		EntityInformation<TypeImplementingPersistable, Long> information = new MappingMongoEntityInformation<>(
				persistableImplementingEntityTypeInfo);

		assertThat(information.isNew(new TypeImplementingPersistable(100L, false))).isFalse();
	}

	static final class TypeImplementingPersistable implements Persistable<Long> {

		private final Long id;
		private final boolean isNew;

		public TypeImplementingPersistable(Long id, boolean isNew) {
			this.id = id;
			this.isNew = isNew;
		}

		public Long getId() {
			return this.id;
		}

		public boolean isNew() {
			return this.isNew;
		}

		public String toString() {
			return "MappingMongoEntityInformationUnitTests.TypeImplementingPersistable(id=" + this.getId() + ", isNew="
					+ this.isNew() + ")";
		}
	}
}
