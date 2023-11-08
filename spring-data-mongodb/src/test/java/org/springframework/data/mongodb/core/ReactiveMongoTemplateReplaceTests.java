/*
 * Copyright 2023 the original author or authors.
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
import static org.springframework.data.mongodb.core.ReplaceOptions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;

import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
public class ReactiveMongoTemplateReplaceTests {

	static final String DB_NAME = "mongo-template-replace-tests";
	static final String RESTAURANT_COLLECTION = "restaurant";

	static @Client MongoClient client;
	private ReactiveMongoTemplate template;

	@BeforeEach
	void beforeEach() {

		template = new ReactiveMongoTemplate(client, DB_NAME);
		template.setEntityLifecycleEventsEnabled(false);

		initTestData();
	}

	@AfterEach()
	void afterEach() {
		clearTestData();
	}

	@Test // GH-4462
	void replacesExistingDocument() {

		Mono<UpdateResult> result = template.replace(query(where("name").is("Central Perk Cafe")),
				new Restaurant("Central Pork Cafe", "Manhattan"));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(1);
			assertThat(it.getModifiedCount()).isEqualTo(1);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 1)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Central Pork Cafe");
				}).verifyComplete();
	}

	@Test // GH-4462
	void replacesFirstOnMoreThanOneMatch() {

		Mono<UpdateResult> result = template.replace(query(where("violations").exists(true)),
				new Restaurant("Central Pork Cafe", "Manhattan"));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(1);
			assertThat(it.getModifiedCount()).isEqualTo(1);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 2)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Central Pork Cafe");
				}).verifyComplete();
	}

	@Test // GH-4462
	void replacesExistingDocumentWithRawDoc() {

		Mono<UpdateResult> result = template.replace(query(where("r-name").is("Central Perk Cafe")),
				Document.parse("{ 'r-name' : 'Central Pork Cafe', 'Borough' : 'Manhattan' }"),
				template.getCollectionName(Restaurant.class));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(1);
			assertThat(it.getModifiedCount()).isEqualTo(1);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 1)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Central Pork Cafe");
				}).verifyComplete();
	}

	@Test // GH-4462
	void replacesExistingDocumentWithRawDocMappingQueryAgainstDomainType() {

		Mono<UpdateResult> result = template.replace(query(where("name").is("Central Perk Cafe")), Restaurant.class,
				Document.parse("{ 'r-name' : 'Central Pork Cafe', 'Borough' : 'Manhattan' }"), ReplaceOptions.none(), template.getCollectionName(Restaurant.class));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(1);
			assertThat(it.getModifiedCount()).isEqualTo(1);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 1)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Central Pork Cafe");
				}).verifyComplete();
	}

	@Test // GH-4462
	void replacesExistingDocumentWithMatchingId() {

		Mono<UpdateResult> result = template.replace(query(where("name").is("Central Perk Cafe")),
				new Restaurant(1L, "Central Pork Cafe", "Manhattan", 0));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(1);
			assertThat(it.getModifiedCount()).isEqualTo(1);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 1)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Central Pork Cafe");
				}).verifyComplete();
	}

	@Test // GH-4462
	void replacesExistingDocumentWithNewIdThrowsDataIntegrityViolationException() {

		template.replace(query(where("name").is("Central Perk Cafe")),
						new Restaurant(4L, "Central Pork Cafe", "Manhattan", 0))
				.as(StepVerifier::create)
				.expectError(DataIntegrityViolationException.class)
				.verify();
	}

	@Test // GH-4462
	void doesNothingIfNoMatchFoundAndUpsertSetToFalse/* by default */() {

		Mono<UpdateResult> result = template.replace(query(where("name").is("Pizza Rat's Pizzaria")),
				new Restaurant(null, "Pizza Rat's Pizzaria", "Manhattan", 8));

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(0);
			assertThat(it.getModifiedCount()).isEqualTo(0);
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("r-name", "Pizza Rat's Pizzaria")).first())
				.as(StepVerifier::create).verifyComplete();
	}

	@Test // GH-4462
	void insertsIfNoMatchFoundAndUpsertSetToTrue() {

		Mono<UpdateResult> result = template.replace(query(where("name").is("Pizza Rat's Pizzaria")),
				new Restaurant(4L, "Pizza Rat's Pizzaria", "Manhattan", 8), replaceOptions().upsert());

		result.as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it.getMatchedCount()).isEqualTo(0);
			assertThat(it.getModifiedCount()).isEqualTo(0);
			assertThat(it.getUpsertedId()).isEqualTo(new BsonInt64(4L));
		}).verifyComplete();

		retrieve(collection -> collection.find(Filters.eq("_id", 4)).first()).as(StepVerifier::create)
				.consumeNextWith(document -> {
					assertThat(document).containsEntry("r-name", "Pizza Rat's Pizzaria");
				});
	}

	void initTestData() {

		List<Document> testData = Stream.of( //
				"{ '_id' : 1, 'r-name' : 'Central Perk Cafe', 'Borough' : 'Manhattan' }",
				"{ '_id' : 2, 'r-name' : 'Rock A Feller Bar and Grill', 'Borough' : 'Queens', 'violations' : 2 }",
				"{ '_id' : 3, 'r-name' : 'Empire State Pub', 'Borough' : 'Brooklyn', 'violations' : 0 }") //
				.map(Document::parse).collect(Collectors.toList());

		doInCollection(collection -> collection.insertMany(testData));
	}

	void clearTestData() {
		doInCollection(collection -> collection.deleteMany(new Document()));
	}

	void doInCollection(Function<MongoCollection<Document>, Publisher<?>> fkt) {
		retrieve(collection -> Mono.from(fkt.apply(collection))).then().as(StepVerifier::create).verifyComplete();
	}

	<T> Mono<T> retrieve(Function<MongoCollection<Document>, Publisher<T>> fkt) {
		return Mono.from(fkt.apply(client.getDatabase(DB_NAME).getCollection(RESTAURANT_COLLECTION)));
	}

	@org.springframework.data.mongodb.core.mapping.Document(RESTAURANT_COLLECTION)
	static class Restaurant {

		Long id;

		@Field("r-name") String name;
		String borough;
		Integer violations;

		Restaurant() {}

		Restaurant(String name, String borough) {

			this.name = name;
			this.borough = borough;
		}

		Restaurant(Long id, String name, String borough, Integer violations) {

			this.id = id;
			this.name = name;
			this.borough = borough;
			this.violations = violations;
		}

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getRName() {
			return name;
		}

		public void setRName(String rName) {
			this.name = rName;
		}

		public String getBorough() {
			return borough;
		}

		public void setBorough(String borough) {
			this.borough = borough;
		}

		public int getViolations() {
			return violations;
		}

		public void setViolations(int violations) {
			this.violations = violations;
		}

		@Override
		public String toString() {
			return "Restaurant{" + "id=" + id + ", name='" + name + '\'' + ", borough='" + borough + '\'' + ", violations="
					+ violations + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Restaurant that = (Restaurant) o;
			return violations == that.violations && Objects.equals(id, that.id) && Objects.equals(name, that.name)
					&& Objects.equals(borough, that.borough);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, borough, violations);
		}
	}

}
