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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.result.DeleteResult;

/**
 * Integration tests for {@link ExecutableRemoveOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
class ExecutableRemoveOperationSupportTests {

	private static final String STAR_WARS = "star-wars";

	@Template(initialEntitySet = Person.class) //
	private static MongoTestTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		template.flush();

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		template.save(han);
		template.save(luke);
	}

	@Test // DATAMONGO-1563
	void removeAll() {

		DeleteResult result = template.remove(Person.class).all();

		assertThat(result.getDeletedCount()).isEqualTo(2L);
	}

	@Test // DATAMONGO-1563
	void removeAllMatching() {

		DeleteResult result = template.remove(Person.class).matching(query(where("firstname").is("han"))).all();

		assertThat(result.getDeletedCount()).isEqualTo(1L);
	}

	@Test // DATAMONGO-2416
	void removeAllMatchingCriteria() {

		DeleteResult result = template.remove(Person.class).matching(where("firstname").is("han")).all();

		assertThat(result.getDeletedCount()).isEqualTo(1L);
	}

	@Test // DATAMONGO-1563
	void removeAllMatchingWithAlternateDomainTypeAndCollection() {

		DeleteResult result = template.remove(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke")))
				.all();

		assertThat(result.getDeletedCount()).isEqualTo(1L);
	}

	@Test // DATAMONGO-1563
	void removeAndReturnAllMatching() {

		List<Person> result = template.remove(Person.class).matching(query(where("firstname").is("han"))).findAndRemove();

		assertThat(result).containsExactly(han);
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

			if (this == o) {
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
			return "ExecutableRemoveOperationSupportTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ")";
		}
	}

	static class Jedi {

		@Field("firstname") //
		String name;

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String toString() {
			return "ExecutableRemoveOperationSupportTests.Jedi(name=" + this.getName() + ")";
		}
	}
}
