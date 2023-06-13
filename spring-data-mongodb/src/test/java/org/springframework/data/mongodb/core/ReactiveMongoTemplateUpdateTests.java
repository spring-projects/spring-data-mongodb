/*
 * Copyright 2019-2023 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.aggregation.AggregationUpdate;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators;
import org.springframework.data.mongodb.core.aggregation.ReplaceWithOperation;
import org.springframework.data.mongodb.core.aggregation.SetOperation;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;

/**
 * @author Christoph Strobl
 */
@ExtendWith(MongoClientExtension.class)
@EnableIfMongoServerVersion(isGreaterThanEqual = "4.2")
public class ReactiveMongoTemplateUpdateTests {

	static final String DB_NAME = "reactive-update-test";

	static @Client MongoClient client;
	ReactiveMongoTemplate template;

	@BeforeEach
	void beforeEach() {

		template = new ReactiveMongoTemplate(new SimpleReactiveMongoDatabaseFactory(client, DB_NAME));

		MongoTestUtils.createOrReplaceCollection(DB_NAME, template.getCollectionName(Score.class), client).then()
				.as(StepVerifier::create).verifyComplete();
		MongoTestUtils.createOrReplaceCollection(DB_NAME, template.getCollectionName(Versioned.class), client).then()
				.as(StepVerifier::create).verifyComplete();
		MongoTestUtils.createOrReplaceCollection(DB_NAME, template.getCollectionName(Book.class), client).then()
				.as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void aggregateUpdateWithSet() {

		Score score1 = new Score(1, "Maya", Arrays.asList(10, 5, 10), Arrays.asList(10, 8), 0);
		Score score2 = new Score(2, "Ryan", Arrays.asList(5, 6, 5), Arrays.asList(8, 8), 8);

		template.insertAll(Arrays.asList(score1, score2)).then().as(StepVerifier::create).verifyComplete();

		AggregationUpdate update = AggregationUpdate.update().set(SetOperation.builder() //
				.set("totalHomework").toValueOf(ArithmeticOperators.valueOf("homework").sum()).and() //
				.set("totalQuiz").toValueOf(ArithmeticOperators.valueOf("quiz").sum())) //
				.set(SetOperation.builder() //
						.set("totalScore")
						.toValueOf(ArithmeticOperators.valueOf("totalHomework").add("totalQuiz").add("extraCredit")));

		template.update(Score.class).apply(update).all().then().as(StepVerifier::create).verifyComplete();

		Flux.from(collection(Score.class).find(new org.bson.Document())).collectList().as(StepVerifier::create)
				.consumeNextWith(it -> {

					assertThat(it).containsExactlyInAnyOrder( //
							org.bson.Document.parse(
									"{\"_id\" : 1, \"student\" : \"Maya\", \"homework\" : [ 10, 5, 10 ], \"quiz\" : [ 10, 8 ], \"extraCredit\" : 0, \"totalHomework\" : 25, \"totalQuiz\" : 18, \"totalScore\" : 43,  \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Score\"}"),
							org.bson.Document.parse(
									"{ \"_id\" : 2, \"student\" : \"Ryan\", \"homework\" : [ 5, 6, 5 ], \"quiz\" : [ 8, 8 ], \"extraCredit\" : 8, \"totalHomework\" : 16, \"totalQuiz\" : 16, \"totalScore\" : 40, \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Score\"}"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void versionedAggregateUpdateWithSet() {

		Versioned source = new Versioned("id-1", "value-0");
		template.insert(Versioned.class).one(source).then().as(StepVerifier::create).verifyComplete();

		AggregationUpdate update = AggregationUpdate.update().set("value").toValue("changed");
		template.update(Versioned.class).matching(Query.query(Criteria.where("id").is(source.id))).apply(update).first()
				.then().as(StepVerifier::create).verifyComplete();

		Flux.from(collection(Versioned.class).find(new org.bson.Document("_id", source.id)).limit(1)).collectList()
				.as(StepVerifier::create).consumeNextWith(it -> {
					assertThat(it).containsExactly(
							new org.bson.Document("_id", source.id).append("version", 1L).append("value", "changed").append("_class",
									"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Versioned"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void versionedAggregateUpdateTouchingVersionProperty() {

		Versioned source = new Versioned("id-1", "value-0");
		template.insert(Versioned.class).one(source).then().as(StepVerifier::create).verifyComplete();

		AggregationUpdate update = AggregationUpdate.update()
				.set(SetOperation.builder().set("value").toValue("changed").and().set("version").toValue(10L));

		template.update(Versioned.class).matching(Query.query(Criteria.where("id").is(source.id))).apply(update).first()
				.then().as(StepVerifier::create).verifyComplete();

		Flux.from(collection(Versioned.class).find(new org.bson.Document("_id", source.id)).limit(1)).collectList()
				.as(StepVerifier::create).consumeNextWith(it -> {
					assertThat(it).containsExactly(
							new org.bson.Document("_id", source.id).append("version", 10L).append("value", "changed").append("_class",
									"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Versioned"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void aggregateUpdateWithUnset() {

		Book antelopeAntics = new Book();
		antelopeAntics.id = 1;
		antelopeAntics.title = "Antelope Antics";
		antelopeAntics.isbn = "0001122223334";
		antelopeAntics.author = new Author("Auntie", "An");
		antelopeAntics.stock = new ArrayList<>();
		antelopeAntics.stock.add(new Warehouse("A", 5));
		antelopeAntics.stock.add(new Warehouse("B", 15));

		Book beesBabble = new Book();
		beesBabble.id = 2;
		beesBabble.title = "Bees Babble";
		beesBabble.isbn = "999999999333";
		beesBabble.author = new Author("Bee", "Bumble");
		beesBabble.stock = new ArrayList<>();
		beesBabble.stock.add(new Warehouse("A", 2));
		beesBabble.stock.add(new Warehouse("B", 5));

		template.insertAll(Arrays.asList(antelopeAntics, beesBabble)).then().as(StepVerifier::create).verifyComplete();

		AggregationUpdate update = AggregationUpdate.update().unset("isbn", "stock");
		template.update(Book.class).apply(update).all().then().as(StepVerifier::create).verifyComplete();

		all(Book.class).collectList().as(StepVerifier::create).consumeNextWith(it -> {

			assertThat(it).containsExactlyInAnyOrder( //
					org.bson.Document.parse(
							"{ \"_id\" : 1, \"title\" : \"Antelope Antics\", \"author\" : { \"last\" : \"An\", \"first\" : \"Auntie\" }, \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\" }"),
					org.bson.Document.parse(
							"{ \"_id\" : 2, \"title\" : \"Bees Babble\", \"author\" : { \"last\" : \"Bumble\", \"first\" : \"Bee\" }, \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\" }"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void aggregateUpdateWithReplaceWith() {

		Book one = new Book();
		one.id = 1;
		one.author = new Author("John", "Backus");

		Book two = new Book();
		two.id = 2;
		two.author = new Author("Grace", "Hopper");

		template.insertAll(Arrays.asList(one, two)).then().as(StepVerifier::create).verifyComplete();

		AggregationUpdate update = AggregationUpdate.update()
				.replaceWith(ReplaceWithOperation.replaceWithValueOf("author"));

		template.update(Book.class).apply(update).all().then().as(StepVerifier::create).verifyComplete();

		all(Book.class).collectList().as(StepVerifier::create).consumeNextWith(it -> {

			assertThat(it).containsExactlyInAnyOrder(
					org.bson.Document.parse("{\"_id\" : 1, \"first\" : \"John\", \"last\" : \"Backus\"}"),
					org.bson.Document.parse("{\"_id\" : 2, \"first\" : \"Grace\", \"last\" : \"Hopper\"}"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void aggregationUpdateUpsertsCorrectly() {

		AggregationUpdate update = AggregationUpdate.update().set("title").toValue("The Burning White");

		template.update(Book.class).matching(Query.query(Criteria.where("id").is(1))).apply(update).upsert().then()
				.as(StepVerifier::create).verifyComplete();

		all(Book.class).collectList().as(StepVerifier::create).consumeNextWith(it -> {
			assertThat(it).containsExactly(org.bson.Document.parse("{\"_id\" : 1, \"title\" : \"The Burning White\" }"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void aggregateUpdateFirstMatch() {

		Book one = new Book();
		one.id = 1;
		one.title = "The Blood Mirror";

		Book two = new Book();
		two.id = 2;
		two.title = "The Broken Eye";

		template.insertAll(Arrays.asList(one, two)).then().as(StepVerifier::create).verifyComplete();

		template.update(Book.class).apply(AggregationUpdate.update().set("title").toValue("The Blinding Knife")).first()
				.then().as(StepVerifier::create).verifyComplete();

		all(Book.class).collectList().as(StepVerifier::create).consumeNextWith(it -> {

			assertThat(it).containsExactly(org.bson.Document.parse(
					"{\"_id\" : 1, \"title\" : \"The Blinding Knife\", \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\"}"),
					org.bson.Document.parse(
							"{\"_id\" : 2, \"title\" : \"The Broken Eye\", \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\"}"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-2331
	public void findAndModifyAppliesAggregationUpdateCorrectly() {

		Book one = new Book();
		one.id = 1;
		one.title = "The Blood Mirror";

		Book two = new Book();
		two.id = 2;
		two.title = "The Broken Eye";

		template.insertAll(Arrays.asList(one, two)).then().as(StepVerifier::create).verifyComplete();

		template.update(Book.class) //
				.matching(Query.query(Criteria.where("id").is(one.id))) //
				.apply(AggregationUpdate.update().set("title").toValue("The Blinding Knife")) //
				.findAndModify() //
				.as(StepVerifier::create) //
				.expectNext(one) //
				.verifyComplete();

		all(Book.class).collectList().as(StepVerifier::create).consumeNextWith(it -> {

			assertThat(it).containsExactly(org.bson.Document.parse(
					"{\"_id\" : 1, \"title\" : \"The Blinding Knife\", \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\"}"),
					org.bson.Document.parse(
							"{\"_id\" : 2, \"title\" : \"The Broken Eye\", \"_class\" : \"org.springframework.data.mongodb.core.ReactiveMongoTemplateUpdateTests$Book\"}"));
		}).verifyComplete();

	}

	private Flux<org.bson.Document> all(Class<?> type) {
		return Flux.from(collection(type).find(new org.bson.Document()));
	}

	private MongoCollection<org.bson.Document> collection(Class<?> type) {
		return client.getDatabase(DB_NAME).getCollection(template.getCollectionName(type));
	}

	@Document("scores")
	static class Score {

		Integer id;
		String student;
		List<Integer> homework;
		List<Integer> quiz;
		Integer extraCredit;

		public Score(Integer id, String student, List<Integer> homework, List<Integer> quiz, Integer extraCredit) {

			this.id = id;
			this.student = student;
			this.homework = homework;
			this.quiz = quiz;
			this.extraCredit = extraCredit;
		}
	}

	static class Versioned {

		String id;
		@Version Long version;
		String value;

		public Versioned(String id, String value) {
			this.id = id;
			this.value = value;
		}
	}

	static class Book {

		@Id Integer id;
		String title;
		String isbn;
		Author author;
		@Field("copies") Collection<Warehouse> stock;

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Book book = (Book) o;
			return Objects.equals(id, book.id) && Objects.equals(title, book.title) && Objects.equals(isbn, book.isbn)
					&& Objects.equals(author, book.author) && Objects.equals(stock, book.stock);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, title, isbn, author, stock);
		}
	}

	static class Author {

		@Field("first") String firstname;
		@Field("last") String lastname;

		public Author(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	static class Warehouse {

		public Warehouse(String location, Integer qty) {
			this.location = location;
			this.qty = qty;
		}

		@Field("warehouse") String location;
		Integer qty;
	}
}
