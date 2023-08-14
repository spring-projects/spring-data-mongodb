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

import java.util.Objects;
import java.util.Optional;

import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;

import com.mongodb.client.result.UpdateResult;

/**
 * Integration tests for {@link ExecutableUpdateOperationSupport}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
class ExecutableUpdateOperationSupportTests {

	private static final String STAR_WARS = "star-wars";

	@Template(initialEntitySet = { Human.class, Jedi.class, Person.class }) //
	private static MongoTestTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		template.remove(Person.class).all();

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
	void domainTypeIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.update(null));
	}

	@Test // DATAMONGO-1563
	void updateIsRequired() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.update(Person.class).apply(null));
	}

	@Test // DATAMONGO-1563
	void collectionIsRequiredOnSet() {
		assertThatIllegalArgumentException().isThrownBy(() -> template.update(Person.class).inCollection(null));
	}

	@Test // DATAMONGO-1563
	void findAndModifyOptionsAreRequiredOnSet() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> template.update(Person.class).apply(new Update()).withOptions(null));
	}

	@Test // DATAMONGO-1563
	void updateFirst() {

		UpdateResult result = template.update(Person.class).apply(new Update().set("firstname", "Han")).first();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	void updateAll() {

		UpdateResult result = template.update(Person.class).apply(new Update().set("firstname", "Han")).all();

		assertThat(result.getModifiedCount()).isEqualTo(2L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	void updateAllMatching() {

		UpdateResult result = template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han"))
				.all();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-2416
	void updateAllMatchingCriteria() {

		UpdateResult result = template.update(Person.class).matching(where("id").is(han.getId()))
				.apply(new Update().set("firstname", "Han"))
				.all();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
	}

	@Test // DATAMONGO-1563
	void updateWithDifferentDomainClassAndCollection() {

		UpdateResult result = template.update(Jedi.class).inCollection(STAR_WARS)
				.matching(query(where("_id").is(han.getId()))).apply(new Update().set("name", "Han")).all();

		assertThat(result.getModifiedCount()).isEqualTo(1L);
		assertThat(result.getUpsertedId()).isNull();
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1719
	void findAndModifyValue() {

		Person result = template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han"))
				.findAndModifyValue();

		assertThat(result).isEqualTo(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	void findAndModify() {

		Optional<Person> result = template.update(Person.class).matching(queryHan())
				.apply(new Update().set("firstname", "Han")).findAndModify();

		assertThat(result).contains(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	void findAndModifyWithDifferentDomainTypeAndCollection() {

		Optional<Jedi> result = template.update(Jedi.class).inCollection(STAR_WARS)
				.matching(query(where("_id").is(han.getId()))).apply(new Update().set("name", "Han")).findAndModify();

		assertThat(result.get()).hasFieldOrPropertyWithValue("name", "han");
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1563
	void findAndModifyWithOptions() {

		Optional<Person> result = template.update(Person.class).matching(queryHan())
				.apply(new Update().set("firstname", "Han")).withOptions(FindAndModifyOptions.options().returnNew(true))
				.findAndModify();

		assertThat(result.get()).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname", "Han");
	}

	@Test // DATAMONGO-1563
	void upsert() {

		UpdateResult result = template.update(Person.class).matching(query(where("id").is("id-3")))
				.apply(new Update().set("firstname", "Chewbacca")).upsert();

		assertThat(result.getModifiedCount()).isEqualTo(0L);
		assertThat(result.getUpsertedId()).isEqualTo(new BsonString("id-3"));
	}

	@Test // DATAMONGO-1827
	void findAndReplaceValue() {

		Person luke = new Person();
		luke.firstname = "Luke";

		Person result = template.update(Person.class).matching(queryHan()).replaceWith(luke).findAndReplaceValue();

		assertThat(result).isEqualTo(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Luke");
	}

	@Test // DATAMONGO-1827
	void findAndReplace() {

		Person luke = new Person();
		luke.firstname = "Luke";

		Optional<Person> result = template.update(Person.class).matching(queryHan()).replaceWith(luke).findAndReplace();

		assertThat(result).contains(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Luke");
	}

	@Test // DATAMONGO-1827
	void findAndReplaceWithCollection() {

		Person luke = new Person();
		luke.firstname = "Luke";

		Optional<Person> result = template.update(Person.class).inCollection(STAR_WARS).matching(queryHan())
				.replaceWith(luke).findAndReplace();

		assertThat(result).contains(han);
		assertThat(template.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Luke");
	}

	@Test // DATAMONGO-1827
	void findAndReplaceWithOptions() {

		Person luke = new Person();
		luke.firstname = "Luke";

		Person result = template.update(Person.class).matching(queryHan()).replaceWith(luke)
				.withOptions(FindAndReplaceOptions.options().returnNew()).findAndReplaceValue();

		assertThat(result).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname", "Luke");
	}

	@Test // GH-4463
	void replace() {

		Person luke = new Person();
		luke.id = han.id;
		luke.firstname = "Luke";

		UpdateResult result = template.update(Person.class).matching(queryHan()).replaceWith(luke).replaceFirst();
		assertThat(result.getModifiedCount()).isEqualTo(1L);
	}

	@Test // GH-4463
	void replaceWithOptions() {

		Person luke = new Person();
		luke.id = "upserted-luke";
		luke.firstname = "Luke";

		UpdateResult result = template.update(Person.class).matching(query(where("firstname")
				.is("c3p0"))).replaceWith(luke).withOptions(ReplaceOptions.replaceOptions().upsert()).replaceFirst();
		assertThat(result.getUpsertedId()).isEqualTo(new BsonString("upserted-luke"));
	}

	@Test // DATAMONGO-1827
	void findAndReplaceWithProjection() {

		Person luke = new Person();
		luke.firstname = "Luke";

		Jedi result = template.update(Person.class).matching(queryHan()).replaceWith(luke).as(Jedi.class)
				.findAndReplaceValue();

		assertThat(result.getName()).isEqualTo(han.firstname);
	}

	private Query queryHan() {
		return query(where("id").is(han.getId()));
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
			return "ExecutableUpdateOperationSupportTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ")";
		}
	}

	static class Human {

		@Id String id;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String toString() {
			return "ExecutableUpdateOperationSupportTests.Human(id=" + this.getId() + ")";
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
			return "ExecutableUpdateOperationSupportTests.Jedi(name=" + this.getName() + ")";
		}
	}
}
