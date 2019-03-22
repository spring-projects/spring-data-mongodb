/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Wither;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.MongoTemplateTests.Address;
import org.springframework.data.mongodb.core.MongoTemplateTests.PersonWithConvertedId;
import org.springframework.data.mongodb.core.MongoTemplateTests.VersionedPerson;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperationsAdapter;
import org.springframework.data.mongodb.core.mapping.event.AbstractMongoEventListener;
import org.springframework.data.mongodb.core.mapping.event.AfterSaveEvent;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.mongodb.WriteConcern;

/**
 * Integration test for {@link MongoTemplate}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMongoTemplateTests {

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Autowired SimpleReactiveMongoDatabaseFactory factory;
	@Autowired ReactiveMongoTemplate template;
	@Autowired ConfigurableApplicationContext context;

	@Before
	public void setUp() {

		Flux.merge(template.dropCollection("people"), //
				template.dropCollection("personX"), //
				template.dropCollection("collection"), //
				template.dropCollection(Person.class), //
				template.dropCollection(Venue.class), //
				template.dropCollection(PersonWithAList.class), //
				template.dropCollection(PersonWithIdPropertyOfTypeObjectId.class), //
				template.dropCollection(PersonWithVersionPropertyOfTypeInteger.class), //
				template.dropCollection(Sample.class), //
				template.dropCollection(MyPerson.class)) //
				.as(StepVerifier::create).verifyComplete();
	}

	@After
	public void cleanUp() {}

	@Test // DATAMONGO-1444
	public void insertSetsId() {

		PersonWithAList person = new PersonWithAList();
		assert person.getId() == null;

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	public void insertAllSetsId() {

		PersonWithAList person = new PersonWithAList();

		template.insertAll(Collections.singleton(person)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	public void insertCollectionSetsId() {

		PersonWithAList person = new PersonWithAList();

		template.insert(Collections.singleton(person), PersonWithAList.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	public void saveSetsId() {

		PersonWithAList person = new PersonWithAList();
		assert person.getId() == null;

		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	public void insertsSimpleEntityCorrectly() {

		Person person = new Person("Mark");
		person.setAge(35);
		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.find(new Query(where("_id").is(person.getId())), Person.class) //
				.as(StepVerifier::create) //
				.expectNext(person) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void simpleInsertDoesNotAllowArrays() {

		thrown.expect(IllegalArgumentException.class);

		Person person = new Person("Mark");
		person.setAge(35);
		template.insert(new Person[] { person });
	}

	@Test // DATAMONGO-1444
	public void simpleInsertDoesNotAllowCollections() {

		thrown.expect(IllegalArgumentException.class);

		Person person = new Person("Mark");
		person.setAge(35);
		template.insert(Collections.singletonList(person));
	}

	@Test // DATAMONGO-1444
	public void insertsSimpleEntityWithSuppliedCollectionNameCorrectly() {

		Person person = new Person("Homer");
		person.setAge(35);
		template.insert(person, "people") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.find(new Query(where("_id").is(person.getId())), Person.class, "people") //
				.as(StepVerifier::create) //
				.expectNext(person) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		template.insertAll(people) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.find(new Query().with(Sort.by("firstname")), Person.class) //
				.as(StepVerifier::create) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchWithSuppliedCollectionNameCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		template.insert(people, "people") //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.find(new Query().with(Sort.by("firstname")), Person.class, "people") //
				.as(StepVerifier::create) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchWithSuppliedEntityTypeCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		template.insert(people, Person.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.find(new Query().with(Sort.by("firstname")), Person.class) //
				.as(StepVerifier::create) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testAddingToList() {

		PersonWithAList person = createPersonWithAList("Sven", 22);
		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Query query = new Query(where("id").is(person.getId()));

		template.findOne(query, PersonWithAList.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getWishList()).isEmpty();
				}).verifyComplete();

		person.addToWishList("please work!");

		template.save(person).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.findOne(query, PersonWithAList.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getWishList()).hasSize(1);
				}).verifyComplete();

		Friend friend = new Friend();
		person.setFirstName("Erik");
		person.setAge(21);

		person.addFriend(friend);
		template.save(person).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.findOne(query, PersonWithAList.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getWishList()).hasSize(1);
					assertThat(actual.getFriends()).hasSize(1);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testFindOneWithSort() {

		PersonWithAList sven = createPersonWithAList("Sven", 22);
		PersonWithAList erik = createPersonWithAList("Erik", 21);
		PersonWithAList mark = createPersonWithAList("Mark", 40);

		template.insertAll(Arrays.asList(sven, erik, mark)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		// test query with a sort
		Query query = new Query(where("age").gt(10));
		query.with(Sort.by(Direction.DESC, "age"));

		template.findOne(query, PersonWithAList.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName()).isEqualTo("Mark");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void bogusUpdateDoesNotTriggerException() {

		ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(factory);
		mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person oliver = new Person("Oliver2", 25);
		template.insert(oliver) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Query q = new Query(where("BOGUS").gt(22));
		Update u = new Update().set("firstName", "Sven");

		mongoTemplate.updateFirst(q, u, Person.class) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateFirstByEntityTypeShouldUpdateObject() {

		Person person = new Person("Oliver2", 25);
		template.insert(person) //
				.then(template.updateFirst(new Query(where("age").is(25)), new Update().set("firstName", "Sven"), Person.class)) //
				.flatMapMany(p -> template.find(new Query(where("age").is(25)), Person.class)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName()).isEqualTo("Sven");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateFirstByCollectionNameShouldUpdateObjects() {

		Person person = new Person("Oliver2", 25);
		template.insert(person, "people") //
				.then(template.updateFirst(new Query(where("age").is(25)), new Update().set("firstName", "Sven"), "people")) //
				.flatMapMany(p -> template.find(new Query(where("age").is(25)), Person.class, "people")) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName()).isEqualTo("Sven");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateMultiByEntityTypeShouldUpdateObjects() {

		Query query = new Query(
				new Criteria().orOperator(where("firstName").is("Walter Jr"), where("firstName").is("Walter")));

		template
				.insertAll(
						Mono.just(Arrays.asList(new Person("Walter", 50), new Person("Skyler", 43), new Person("Walter Jr", 16)))) //
				.flatMap(a -> template.updateMulti(query, new Update().set("firstName", "Walt"), Person.class)) //
				.thenMany(template.find(new Query(where("firstName").is("Walt")), Person.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateMultiByCollectionNameShouldUpdateObject() {

		Query query = new Query(
				new Criteria().orOperator(where("firstName").is("Walter Jr"), where("firstName").is("Walter")));

		List<Person> people = Arrays.asList(new Person("Walter", 50), //
				new Person("Skyler", 43), //
				new Person("Walter Jr", 16));

		Flux<Person> personFlux = template.insertAll(Mono.just(people), "people") //
				.collectList() //
				.flatMap(a -> template.updateMulti(query, new Update().set("firstName", "Walt"), Person.class, "people")) //
				.flatMapMany(p -> template.find(new Query(where("firstName").is("Walt")), Person.class, "people"));

		personFlux //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForDuplicateIds() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectError(DataIntegrityViolationException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForUpdateWithInvalidPushOperator() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Query query = new Query(where("firstName").is("Amol"));
		Update upd = new Update().push("age", 29);

		template.updateFirst(query, upd, Person.class) //
				.as(StepVerifier::create) //
				.verifyError(DataIntegrityViolationException.class);
	}

	@Test // DATAMONGO-1444
	public void rejectsDuplicateIdInInsertAll() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		template.insertAll(Arrays.asList(person, person)) //
				.as(StepVerifier::create) //
				.verifyError(DataIntegrityViolationException.class);
	}

	@Test // DATAMONGO-1444
	public void testFindAndUpdate() {

		template.insertAll(Arrays.asList(new Person("Tom", 21), new Person("Dick", 22), new Person("Harry", 23))) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Query query = new Query(where("firstName").is("Harry"));
		Update update = new Update().inc("age", 1);

		Person p = template.findAndModify(query, update, Person.class).block(); // return old
		assertThat(p.getFirstName()).isEqualTo("Harry");
		assertThat(p.getAge()).isEqualTo(23);
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge()).isEqualTo(24);

		p = template.findAndModify(query, update, Person.class, "person").block();
		assertThat(p.getAge()).isEqualTo(24);
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge()).isEqualTo(25);

		p = template.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Person.class).block();
		assertThat(p.getAge()).isEqualTo(26);

		p = template.findAndModify(query, update, null, Person.class, "person").block();
		assertThat(p.getAge()).isEqualTo(26);
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge()).isEqualTo(27);

		Query query2 = new Query(where("firstName").is("Mary"));
		p = template.findAndModify(query2, update, new FindAndModifyOptions().returnNew(true).upsert(true), Person.class)
				.block();
		assertThat(p.getFirstName()).isEqualTo("Mary");
		assertThat(p.getAge()).isEqualTo(1);
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceDocument() {

		org.bson.Document doc = new org.bson.Document("foo", "bar");
		template.save(doc, "findandreplace").as(StepVerifier::create).expectNextCount(1).verifyComplete();

		org.bson.Document replacement = new org.bson.Document("foo", "baz");
		template
				.findAndReplace(query(where("foo").is("bar")), replacement, FindAndReplaceOptions.options(),
						org.bson.Document.class, "findandreplace") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).containsEntry("foo", "bar");
				}).verifyComplete();

		template.findOne(query(where("foo").is("baz")), org.bson.Document.class, "findandreplace") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldErrorOnIdPresent() {

		template.save(new MyPerson("Walter")).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		MyPerson replacement = new MyPerson("Heisenberg");
		replacement.id = "invalid-id";

		template.findAndReplace(query(where("name").is("Walter")), replacement) //
				.as(StepVerifier::create) //
				.expectError(InvalidDataAccessApiUsageException.class);
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldErrorOnSkip() {

		thrown.expect(IllegalArgumentException.class);

		template.findAndReplace(query(where("name").is("Walter")).skip(10), new MyPerson("Heisenberg")).subscribe();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldErrorOnLimit() {

		thrown.expect(IllegalArgumentException.class);

		template.findAndReplace(query(where("name").is("Walter")).limit(10), new MyPerson("Heisenberg")).subscribe();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldConsiderSortAndUpdateFirstIfMultipleFound() {

		MyPerson walter1 = new MyPerson("Walter 1");
		MyPerson walter2 = new MyPerson("Walter 2");

		template.save(walter1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		template.save(walter2).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		MyPerson replacement = new MyPerson("Heisenberg");

		template.findAndReplace(query(where("name").regex("Walter.*")).with(Sort.by(Direction.DESC, "name")), replacement)
				.as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.findAll(MyPerson.class).buffer(10).as(StepVerifier::create)
				.consumeNextWith(it -> assertThat(it).hasSize(2).contains(walter1).doesNotContain(walter2)).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceObject() {

		MyPerson person = new MyPerson("Walter");
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg")) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("Walter");
				}).verifyComplete();

		template.findOne(query(where("name").is("Heisenberg")), MyPerson.class) //
				.as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldConsiderFields() {

		MyPerson person = new MyPerson("Walter");
		person.address = new Address("TX", "Austin");
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Query query = query(where("name").is("Walter"));
		query.fields().include("address");

		template.findAndReplace(query, new MyPerson("Heisenberg")) //
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {

					assertThat(it.getName()).isNull();
					assertThat(it.getAddress()).isEqualTo(person.address);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceNonExistingWithUpsertFalse() {

		template.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg")) //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.findAll(MyPerson.class).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceNonExistingWithUpsertTrue() {

		template
				.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"),
						FindAndReplaceOptions.options().upsert()) //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.findAll(MyPerson.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldProjectReturnedObjectCorrectly() {

		MyPerson person = new MyPerson("Walter");
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template
				.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"), FindAndReplaceOptions.empty(),
						MyPerson.class, MyPersonProjection.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("Walter");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1827
	public void findAndReplaceShouldReplaceObjectReturingNew() {

		MyPerson person = new MyPerson("Walter");
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template
				.findAndReplace(query(where("name").is("Walter")), new MyPerson("Heisenberg"),
						FindAndReplaceOptions.options().returnNew())
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("Heisenberg");
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testFindAllAndRemoveFullyReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		template.insert(Arrays.asList(spring, data, mongodb), Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Query qry = query(where("field").in("spring", "mongodb"));

		template.findAllAndRemove(qry, Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		template.findOne(new Query(), Sample.class) //
				.as(StepVerifier::create) //
				.expectNext(data) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2219
	public void testFindAllAndRemoveReturnsEmptyWithoutMatches() {

		Query qry = query(where("field").in("spring", "mongodb"));
		template.findAllAndRemove(qry, Sample.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.count(new Query(), Sample.class) //
				.as(StepVerifier::create) //
				.expectNext(0L).verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void testFindAllAndRemoveByCollectionReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		template.insert(Arrays.asList(spring, data, mongodb), Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Query qry = query(where("field").in("spring", "mongodb"));

		template.findAllAndRemove(qry, "sample") //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		template.findOne(new Query(), Sample.class) //
				.as(StepVerifier::create) //
				.expectNext(data) //
				.verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1774
	public void removeWithNullShouldThrowError() {
		template.remove((Object) null).subscribe();
	}

	@Test // DATAMONGO-1774
	public void removeWithEmptyMonoShouldDoNothing() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		template.insert(Arrays.asList(spring, data, mongodb), Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.remove(Mono.empty()).as(StepVerifier::create).verifyComplete();
		template.count(new Query(), Sample.class) //
				.as(StepVerifier::create) //
				.expectNext(3L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void removeWithMonoShouldDeleteElement() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		template.insert(Arrays.asList(spring, data, mongodb), Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.remove(Mono.just(spring)).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		template.count(new Query(), Sample.class).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void removeWithMonoAndCollectionShouldDeleteElement() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		template.insert(Arrays.asList(spring, data, mongodb), Sample.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.remove(Mono.just(spring), template.determineCollectionName(Sample.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();
		template.count(new Query(), Sample.class).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-2195
	public void removeVersionedEntityConsidersVersion() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		assertThat(person.version).isZero();

		template.update(PersonWithVersionPropertyOfTypeInteger.class).matching(query(where("id").is(person.id)))
				.apply(new Update().set("firstName", "Walter")).first() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.remove(person).as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.wasAcknowledged()).isTrue();
					assertThat(actual.getDeletedCount()).isZero();
				}).verifyComplete();
		template.count(new Query(), PersonWithVersionPropertyOfTypeInteger.class) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void optimisticLockingHandling() {

		// Init version
		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.age = 29;
		person.firstName = "Patryk";

		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findAll(PersonWithVersionPropertyOfTypeInteger.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.version).isZero();
				}).verifyComplete();

		template.findAll(PersonWithVersionPropertyOfTypeInteger.class).flatMap(p -> {

			// Version change
			person.firstName = "Patryk2";
			return template.save(person);
		}) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		assertThat(person.version).isOne();

		template.findAll(PersonWithVersionPropertyOfTypeInteger.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.version).isOne();
				}).verifyComplete();

		// Optimistic lock exception
		person.version = 0;
		person.firstName = "Patryk3";

		template.save(person).as(StepVerifier::create).expectError(OptimisticLockingFailureException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void doesNotFailOnVersionInitForUnversionedEntity() {

		Document dbObject = new Document();
		dbObject.put("firstName", "Oliver");

		template.insert(dbObject, //
				template.determineCollectionName(PersonWithVersionPropertyOfTypeInteger.class)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void removesObjectFromExplicitCollection() {

		String collectionName = "explicit";
		template.remove(new Query(), collectionName).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		PersonWithConvertedId person = new PersonWithConvertedId();
		person.name = "Dave";

		template.save(person, collectionName) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findAll(PersonWithConvertedId.class, collectionName) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		template.remove(person, collectionName).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.findAll(PersonWithConvertedId.class, collectionName).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void savesMapCorrectly() {

		Map<String, String> map = new HashMap<>();
		map.put("key", "value");

		template.save(map, "maps") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test(expected = MappingException.class) // DATAMONGO-1444, DATAMONGO-1730, DATAMONGO-2150
	public void savesMongoPrimitiveObjectCorrectly() {
		template.save(new Object(), "collection");
	}

	@Test // DATAMONGO-1444
	public void savesPlainDbObjectCorrectly() {

		Document dbObject = new Document("foo", "bar");

		template.save(dbObject, "collection") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(dbObject.containsKey("_id")).isTrue();
	}

	@Test(expected = MappingException.class) // DATAMONGO-1444, DATAMONGO-1730
	public void rejectsPlainObjectWithOutExplicitCollection() {

		Document dbObject = new Document("foo", "bar");

		template.save(dbObject, "collection") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findById(dbObject.get("_id"), Document.class) //
				.as(StepVerifier::create) //
				.verifyError(MappingException.class);
	}

	@Test // DATAMONGO-1444
	public void readsPlainDbObjectById() {

		Document dbObject = new Document("foo", "bar");
		template.save(dbObject, "collection") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findById(dbObject.get("_id"), Document.class, "collection") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.get("foo")).isEqualTo(dbObject.get("foo"));
					assertThat(actual.get("_id")).isEqualTo(dbObject.get("_id"));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void geoNear() {

		List<Venue> venues = Arrays.asList(TestEntities.geolocation().pennStation(), //
				TestEntities.geolocation().tenGenOffice(), //
				TestEntities.geolocation().flatironBuilding(), //
				TestEntities.geolocation().maplewoodNJ());

		template.insertAll(venues) //
				.as(StepVerifier::create) //
				.expectNextCount(4) //
				.verifyComplete();

		IndexOperationsAdapter.blocking(template.indexOps(Venue.class))
				.ensureIndex(new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2D));

		NearQuery geoFar = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(150, Metrics.KILOMETERS);

		template.geoNear(geoFar, Venue.class) //
				.as(StepVerifier::create) //
				.expectNextCount(4) //
				.verifyComplete();

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(120, Metrics.KILOMETERS);

		template.geoNear(geoNear, Venue.class) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void writesPlainString() {

		template.save("{ 'foo' : 'bar' }", "collection") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test(expected = MappingException.class) // DATAMONGO-1444, DATAMONGO-2150
	public void rejectsNonJsonStringForSave() {
		template.save("Foobar!", "collection");
	}

	@Test // DATAMONGO-1444
	public void initializesVersionOnInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insert(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.version).isZero();
	}

	@Test // DATAMONGO-1444
	public void initializesVersionOnBatchInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.insertAll(Collections.singleton(person)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.version).isZero();
	}

	@Test // DATAMONGO-1992
	public void initializesIdAndVersionAndOfImmutableObject() {

		ImmutableVersioned versioned = new ImmutableVersioned();

		template.insert(versioned) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).isNotSameAs(versioned);
					assertThat(versioned.id).isNull();
					assertThat(versioned.version).isNull();

					assertThat(actual.id).isNotNull();
					assertThat(actual.version).isEqualTo(0);

				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void queryCanBeNull() {

		template.findAll(PersonWithIdPropertyOfTypeObjectId.class) //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.find(null, PersonWithIdPropertyOfTypeObjectId.class) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void versionsObjectIntoDedicatedCollection() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person, "personX") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		assertThat(person.version).isZero();

		template.save(person, "personX") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		assertThat(person.version).isOne();
	}

	@Test // DATAMONGO-1444
	public void correctlySetsLongVersionProperty() {

		PersonWithVersionPropertyOfTypeLong person = new PersonWithVersionPropertyOfTypeLong();
		person.firstName = "Dave";

		template.save(person, "personX") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		assertThat(person.version).isZero();
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForIndexViolationIfConfigured() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
		template.indexOps(Person.class) //
				.ensureIndex(new Index().on("firstName", Direction.DESC).unique()) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		template.save(person) //
				.as(StepVerifier::create) //
				.verifyError(DataIntegrityViolationException.class);
	}

	@Test // DATAMONGO-1444
	public void preventsDuplicateInsert() {

		template.setWriteConcern(WriteConcern.MAJORITY);

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		assertThat(person.version).isZero();

		person.version = null;
		template.save(person) //
				.as(StepVerifier::create) //
				.verifyError(DuplicateKeyException.class);
	}

	@Test // DATAMONGO-1444
	public void countAndFindWithoutTypeInformation() {

		Person person = new Person();
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Query query = query(where("_id").is(person.getId()));
		String collectionName = template.getCollectionName(Person.class);

		template.find(query, HashMap.class, collectionName) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.count(query, collectionName) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void nullsPropertiesForVersionObjectUpdates() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";

		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(person.id).isNotNull();

		person.lastname = null;
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findOne(query(where("id").is(person.id)), VersionedPerson.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.lastname).isNull();
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void nullsValuesForUpdatesOfUnversionedEntity() {

		Person person = new Person("Dave");
		template.save(person). //
				as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		person.setFirstName(null);
		template.save(person) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findOne(query(where("id").is(person.getId())), Person.class) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName()).isNull();
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void savesJsonStringCorrectly() {

		Document dbObject = new Document().append("first", "first").append("second", "second");

		template.save(dbObject, "collection") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.findAll(Document.class, "collection") //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.containsKey("first")).isTrue();
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executesExistsCorrectly() {

		Sample sample = new Sample();
		template.save(sample).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		Query query = query(where("id").is(sample.id));

		template.exists(query, Sample.class) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		template.exists(query(where("_id").is(sample.id)), template.getCollectionName(Sample.class)) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		template.exists(query, Sample.class, template.getCollectionName(Sample.class)) //
				.as(StepVerifier::create).expectNext(true) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void tailStreamsData() throws InterruptedException {

		template.dropCollection("capped").then(template.createCollection("capped", //
				CollectionOptions.empty().size(1000).maxDocuments(10).capped()))
				.then(template.insert(new Document("random", Math.random()).append("key", "value"), //
						"capped")) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		BlockingQueue<Document> documents = new LinkedBlockingQueue<>(1000);

		Flux<Document> capped = template.tail(null, Document.class, "capped");

		Disposable disposable = capped.doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(documents).isEmpty();

		disposable.dispose();
	}

	@Test // DATAMONGO-1444
	public void tailStreamsDataUntilCancellation() throws InterruptedException {

		template.dropCollection("capped").then(template.createCollection("capped", //
				CollectionOptions.empty().size(1000).maxDocuments(10).capped()))
				.then(template.insert(new Document("random", Math.random()).append("key", "value"), //
						"capped")) //
				.as(StepVerifier::create) //
				.expectNextCount(1).verifyComplete();

		BlockingQueue<Document> documents = new LinkedBlockingQueue<>(1000);

		Flux<Document> capped = template.tail(null, Document.class, "capped");

		Disposable disposable = capped.doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(documents).isEmpty();

		template.insert(new Document("random", Math.random()).append("key", "value"), "capped") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(documents.poll(5, TimeUnit.SECONDS)).isNotNull();

		disposable.dispose();

		template.insert(new Document("random", Math.random()).append("key", "value"), "capped") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(documents.poll(1, TimeUnit.SECONDS)).isNull();
	}

	@Test // DATAMONGO-1761
	public void testDistinct() {

		Person person1 = new Person("Christoph", 38);
		Person person2 = new Person("Christine", 39);
		Person person3 = new Person("Christoph", 37);

		template.insertAll(Arrays.asList(person1, person2, person3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		template.findDistinct("firstName", Person.class, String.class) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1803
	public void changeStreamEventsShouldBeEmittedCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Document>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person", ChangeStreamOptions.empty(), Document.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 39);
		Person person3 = new Person("MongoDB", 37);


		Flux.merge(template.insert(person1), template.insert(person2), template.insert(person3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList())).hasSize(3)
					.allMatch(val -> val instanceof Document);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	public void changeStreamEventsShouldBeConvertedCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person", ChangeStreamOptions.empty(), Person.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 39);
		Person person3 = new Person("MongoDB", 37);

		Flux.merge(template.insert(person1), template.insert(person2), template.insert(person3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(person1, person2, person3);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	public void changeStreamEventsShouldBeFilteredCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person",
				ChangeStreamOptions.builder().filter(newAggregation(Person.class, match(where("age").gte(38)))).build(),
				Person.class).doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		Flux.merge(template.save(person1), template.save(person2), template.save(person3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(person1, person3);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	public void mapsReservedWordsCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template
				.changeStream("person",
						ChangeStreamOptions.builder()
								.filter(newAggregation(Person.class, match(where("operationType").is("replace")))).build(),
						Person.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);


		Flux.merge(template.insert(person1), template.insert(person2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Person replacement = new Person(person2.getId(), "BDognoM");
		replacement.setAge(person2.getAge());

		template.save(replacement) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(replacement);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-1803
	public void changeStreamEventsShouldBeResumedCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person", ChangeStreamOptions.empty(), Person.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		Flux.merge(template.insert(person1), template.insert(person2), template.insert(person3)) //
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		disposable.dispose();

		BsonDocument resumeToken = documents.take().getRaw().getResumeToken();

		BlockingQueue<ChangeStreamEvent<Person>> resumeDocuments = new LinkedBlockingQueue<>(100);
		template.changeStream("person", ChangeStreamOptions.builder().resumeToken(resumeToken).build(), Person.class)
				.doOnNext(resumeDocuments::add).subscribe();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(resumeDocuments.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(person2, person3);
		} finally {
			disposable.dispose();
		}

	}

	@Test // DATAMONGO-1870
	public void removeShouldConsiderLimit() {

		List<Sample> samples = IntStream.range(0, 100) //
				.mapToObj(i -> new Sample("id-" + i, i % 2 == 0 ? "stark" : "lannister")) //
				.collect(Collectors.toList());

		template.insertAll(samples) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		template.remove(query(where("field").is("lannister")).limit(25), Sample.class) //
				.as(StepVerifier::create) //
				.assertNext(wr -> Assertions.assertThat(wr.getDeletedCount()).isEqualTo(25L)).verifyComplete();
	}

	@Test // DATAMONGO-1870
	public void removeShouldConsiderSkipAndSort() {

		List<Sample> samples = IntStream.range(0, 100) //
				.mapToObj(i -> new Sample("id-" + i, i % 2 == 0 ? "stark" : "lannister")) //
				.collect(Collectors.toList());

		template.insertAll(samples).as(StepVerifier::create).expectNextCount(100).verifyComplete();

		template.remove(new Query().skip(25).with(Sort.by("field")), Sample.class) //
				.as(StepVerifier::create) //
				.assertNext(wr -> Assertions.assertThat(wr.getDeletedCount()).isEqualTo(75L)).verifyComplete();

		template.count(query(where("field").is("lannister")), Sample.class).as(StepVerifier::create).expectNext(25L)
				.verifyComplete();
		template.count(query(where("field").is("stark")), Sample.class).as(StepVerifier::create).expectNext(0L)
				.verifyComplete();
	}

	@Test // DATAMONGO-2189
	@DirtiesContext
	public void afterSaveEventContainsSavedObjectUsingInsert() {

		AtomicReference<ImmutableVersioned> saved = createAfterSaveReference();
		ImmutableVersioned source = new ImmutableVersioned();

		template.insert(source) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(saved.get()).isNotNull().isNotSameAs(source);
		assertThat(saved.get().id).isNotNull();
	}

	@Test // DATAMONGO-2189
	@DirtiesContext
	public void afterSaveEventContainsSavedObjectUsingInsertAll() {

		AtomicReference<ImmutableVersioned> saved = createAfterSaveReference();
		ImmutableVersioned source = new ImmutableVersioned();

		template.insertAll(Collections.singleton(new ImmutableVersioned())) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(saved.get()).isNotNull().isNotSameAs(source);
		assertThat(saved.get().id).isNotNull();
	}

	@Test // DATAMONGO-2012
	public void watchesDatabaseCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		template.createCollection("personX").as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream(ChangeStreamOptions.empty(), Person.class).doOnNext(documents::add)
				.subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		template.save(person1) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		template.save(person2) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		template.save(person3, "personX") //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(documents.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(person1, person2, person3);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-2012, DATAMONGO-2113
	public void resumesAtTimestampCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person", ChangeStreamOptions.empty(), Person.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		template.save(person1).delayElement(Duration.ofSeconds(1)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //

				.verifyComplete();
		template.save(person2) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Thread.sleep(500); // just give it some time to link receive all events

		disposable.dispose();

		documents.take(); // skip first
		Instant resumeAt = documents.take().getTimestamp(); // take 2nd

		template.save(person3).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> resumeDocuments = new LinkedBlockingQueue<>(100);
		template.changeStream("person", ChangeStreamOptions.builder().resumeAt(resumeAt).build(), Person.class)
				.doOnNext(resumeDocuments::add).subscribe();

		Thread.sleep(500); // just give it some time to link receive all events

		try {
			Assertions.assertThat(resumeDocuments.stream().map(ChangeStreamEvent::getBody).collect(Collectors.toList()))
					.containsExactly(person2, person3);
		} finally {
			disposable.dispose();
		}
	}

	@Test // DATAMONGO-2115
	public void resumesAtBsonTimestampCorrectly() throws InterruptedException {

		Assumptions.assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		template.createCollection(Person.class).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<ChangeStreamEvent<Person>> documents = new LinkedBlockingQueue<>(100);
		Disposable disposable = template.changeStream("person", ChangeStreamOptions.empty(), Person.class)
				.doOnNext(documents::add).subscribe();

		Thread.sleep(500); // just give it some time to link to the collection.

		Person person1 = new Person("Spring", 38);
		Person person2 = new Person("Data", 37);
		Person person3 = new Person("MongoDB", 39);

		template.save(person1).delayElement(Duration.ofSeconds(1)) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
		template.save(person2) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		documents.take(); // skip first
		BsonTimestamp resumeAt = documents.take().getBsonTimestamp(); // take 2nd

		disposable.dispose();

		template.save(person3).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		template.changeStream("person", ChangeStreamOptions.builder().resumeAt(resumeAt).build(), Person.class)
				.map(ChangeStreamEvent::getBody) //
				.buffer(2) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).containsExactly(person2, person3);
				}).thenCancel() //
				.verify();
	}

	private PersonWithAList createPersonWithAList(String firstname, int age) {

		PersonWithAList p = new PersonWithAList();
		p.setFirstName(firstname);
		p.setAge(age);

		return p;
	}

	private AtomicReference<ImmutableVersioned> createAfterSaveReference() {

		AtomicReference<ImmutableVersioned> saved = new AtomicReference<>();
		context.addApplicationListener(new AbstractMongoEventListener<ImmutableVersioned>() {

			@Override
			public void onAfterSave(AfterSaveEvent<ImmutableVersioned> event) {
				saved.set(event.getSource());
			}
		});

		return saved;
	}

	@AllArgsConstructor
	@Wither
	static class ImmutableVersioned {

		final @Id String id;
		final @Version Long version;

		public ImmutableVersioned() {
			id = null;
			version = null;
		}
	}

	@Data
	static class Sample {

		@Id String id;
		String field;

		public Sample() {}

		public Sample(String id, String field) {
			this.id = id;
			this.field = field;
		}
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class MyPerson {

		String id;
		String name;
		Address address;

		public MyPerson(String name) {
			this.name = name;
		}
	}

	interface MyPersonProjection {
		String getName();
	}
}
