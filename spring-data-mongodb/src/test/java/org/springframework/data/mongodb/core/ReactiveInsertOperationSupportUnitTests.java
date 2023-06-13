/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.annotation.Id;

/**
 * Unit tests for {@link ExecutableInsertOperationSupport}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
public class ReactiveInsertOperationSupportUnitTests {

	private static final String STAR_WARS = "star-wars";

	@Mock ReactiveMongoTemplate template;

	private ReactiveInsertOperationSupport ops;

	private Person luke, han;

	@BeforeEach
	void setUp() {

		ops = new ReactiveInsertOperationSupport(template);

		luke = new Person();
		luke.id = "id-1";
		luke.firstname = "luke";

		han = new Person();
		han.firstname = "han";
		han.id = "id-2";
	}

	@Test // DATAMONGO-1719
	void nullCollectionShouldThrowException() {
		assertThatIllegalArgumentException().isThrownBy(() -> ops.insert(Person.class).inCollection(null));
	}

	@Test // DATAMONGO-1719
	void insertShouldUseDerivedCollectionName() {

		when(template.getCollectionName(any(Class.class))).thenReturn(STAR_WARS);

		ops.insert(Person.class).one(luke);

		ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);

		verify(template).getCollectionName(captor.capture());
		verify(template).insert(eq(luke), eq(STAR_WARS));

		assertThat(captor.getAllValues()).containsExactly(Person.class);
	}

	@Test // DATAMONGO-1719
	void insertShouldUseExplicitCollectionName() {

		ops.insert(Person.class).inCollection(STAR_WARS).one(luke);

		verify(template, never()).getCollectionName(any(Class.class));
		verify(template).insert(eq(luke), eq(STAR_WARS));
	}

	@Test // DATAMONGO-1719
	void insertCollectionShouldDelegateCorrectly() {

		when(template.getCollectionName(any(Class.class))).thenReturn(STAR_WARS);

		ops.insert(Person.class).all(Arrays.asList(luke, han));

		verify(template).getCollectionName(any(Class.class));
		verify(template).insert(anyList(), eq(STAR_WARS));
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person {

		@Id String id;
		String firstname;

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Person person = (Person) o;
			return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname);
		}

		public String toString() {
			return "ReactiveInsertOperationSupportUnitTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ")";
		}
	}
}
