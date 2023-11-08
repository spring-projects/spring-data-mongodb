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

import reactor.test.StepVerifier;

import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for {@link ReactiveRemoveOperationSupport}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
class ReactiveRemoveOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	private static @Client MongoClient client;
	private static @Client com.mongodb.reactivestreams.client.MongoClient reactiveClient;

	private MongoTemplate blocking;
	private ReactiveMongoTemplate template;

	private Person han;
	private Person luke;

	@BeforeEach
	void setUp() {

		blocking = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, "ExecutableRemoveOperationSupportTests"));
		blocking.dropCollection(STAR_WARS);

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		blocking.save(han);
		blocking.save(luke);

		template = new ReactiveMongoTemplate(reactiveClient, "ExecutableRemoveOperationSupportTests");
	}

	@Test // DATAMONGO-1719
	void removeAll() {

		template.remove(Person.class).all().as(StepVerifier::create).consumeNextWith(actual -> {
			assertThat(actual.getDeletedCount()).isEqualTo(2L);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	void removeAllMatching() {

		template.remove(Person.class).matching(query(where("firstname").is("han"))).all().as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getDeletedCount()).isEqualTo(1L)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	void removeAllMatchingCriteria() {

		template.remove(Person.class).matching(where("firstname").is("han")).all().as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getDeletedCount()).isEqualTo(1L)).verifyComplete();
	}

	@Test // DATAMONGO-1719
	void removeAllMatchingWithAlternateDomainTypeAndCollection() {

		template.remove(Jedi.class).inCollection(STAR_WARS).matching(query(where("name").is("luke"))).all()
				.as(StepVerifier::create).consumeNextWith(actual -> assertThat(actual.getDeletedCount()).isEqualTo(1L))
				.verifyComplete();
	}

	@Test // DATAMONGO-1719
	void removeAndReturnAllMatching() {

		template.remove(Person.class).matching(query(where("firstname").is("han"))).findAndRemove().as(StepVerifier::create)
				.expectNext(han).verifyComplete();
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
			return "ReactiveRemoveOperationSupportTests.Person(id=" + this.getId() + ", firstname=" + this.getFirstname()
					+ ")";
		}
	}

	static class Jedi {

		@Field("firstname") String name;

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Jedi jedi = (Jedi) o;
			return Objects.equals(name, jedi.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name);
		}

		public String toString() {
			return "ReactiveRemoveOperationSupportTests.Jedi(name=" + this.getName() + ")";
		}
	}
}
