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
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;
import reactor.test.StepVerifier;

import org.bson.BsonString;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

/**
 * Integration tests for {@link ReactiveUpdateOperationSupport}.
 *
 * @author Mark Paluch
 */
public class ReactiveUpdateOperationSupportTests {

	private static final String STAR_WARS = "star-wars";
	MongoTemplate blocking;
	ReactiveMongoTemplate template;

	Person han;
	Person luke;

	@Before
	public void setUp() {

		blocking = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "ExecutableUpdateOperationSupportTests"));
		blocking.dropCollection(STAR_WARS);

		han = new Person();
		han.firstname = "han";
		han.id = "id-1";

		luke = new Person();
		luke.firstname = "luke";
		luke.id = "id-2";

		blocking.save(han);
		blocking.save(luke);

		template = new ReactiveMongoTemplate(MongoClients.create(), "ExecutableUpdateOperationSupportTests");
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void domainTypeIsRequired() {
		template.update(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void updateIsRequired() {
		template.update(Person.class).apply(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void collectionIsRequiredOnSet() {
		template.update(Person.class).inCollection(null);
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1719
	public void findAndModifyOptionsAreRequiredOnSet() {
		template.update(Person.class).apply(new Update()).withOptions(null);
	}

	@Test // DATAMONGO-1719
	public void updateFirst() {

		StepVerifier.create(template.update(Person.class).apply(new Update().set("firstname", "Han")).first())
				.consumeNextWith(actual -> {

					assertThat(actual.getModifiedCount()).isEqualTo(1L);
					assertThat(actual.getUpsertedId()).isNull();
				}).verifyComplete();

	}

	@Test // DATAMONGO-1719
	public void updateAll() {

		StepVerifier.create(template.update(Person.class).apply(new Update().set("firstname", "Han")).all())
				.consumeNextWith(actual -> {

					assertThat(actual.getModifiedCount()).isEqualTo(2L);
					assertThat(actual.getUpsertedId()).isNull();
				}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void updateAllMatching() {

		StepVerifier
				.create(template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han")).all())
				.consumeNextWith(actual -> {

					assertThat(actual.getModifiedCount()).isEqualTo(1L);
					assertThat(actual.getUpsertedId()).isNull();
				}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void updateWithDifferentDomainClassAndCollection() {

		StepVerifier.create(template.update(Jedi.class).inCollection(STAR_WARS)
				.matching(query(where("_id").is(han.getId()))).apply(new Update().set("name", "Han")).all())
				.consumeNextWith(actual -> {

					assertThat(actual.getModifiedCount()).isEqualTo(1L);
					assertThat(actual.getUpsertedId()).isNull();
				}).verifyComplete();

		assertThat(blocking.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1719
	public void findAndModify() {

		StepVerifier.create(
				template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han")).findAndModify())
				.expectNext(han).verifyComplete();

		assertThat(blocking.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1719
	public void findAndModifyWithDifferentDomainTypeAndCollection() {

		StepVerifier
				.create(template.update(Jedi.class).inCollection(STAR_WARS).matching(query(where("_id").is(han.getId())))
						.apply(new Update().set("name", "Han")).findAndModify())
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("han")).verifyComplete();

		assertThat(blocking.findOne(queryHan(), Person.class)).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname",
				"Han");
	}

	@Test // DATAMONGO-1719
	public void findAndModifyWithOptions() {

		StepVerifier.create(template.update(Person.class).matching(queryHan()).apply(new Update().set("firstname", "Han"))
				.withOptions(FindAndModifyOptions.options().returnNew(true)).findAndModify()).consumeNextWith(actual -> {

					assertThat(actual).isNotEqualTo(han).hasFieldOrPropertyWithValue("firstname", "Han");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1719
	public void upsert() {

		StepVerifier.create(template.update(Person.class).matching(query(where("id").is("id-3")))
				.apply(new Update().set("firstname", "Chewbacca")).upsert()).consumeNextWith(actual -> {

					assertThat(actual.getModifiedCount()).isEqualTo(0L);
					assertThat(actual.getUpsertedId()).isEqualTo(new BsonString("id-3"));
				}).verifyComplete();
	}

	private Query queryHan() {
		return query(where("id").is(han.getId()));
	}

	@Data
	@org.springframework.data.mongodb.core.mapping.Document(collection = STAR_WARS)
	static class Person {
		@Id String id;
		String firstname;
	}

	@Data
	static class Human {
		@Id String id;
	}

	@Data
	static class Jedi {

		@Field("firstname") String name;
	}
}
