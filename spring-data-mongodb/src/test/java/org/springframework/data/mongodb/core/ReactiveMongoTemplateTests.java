/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import lombok.Data;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Metrics;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mongodb.core.MongoTemplateTests.PersonWithConvertedId;
import org.springframework.data.mongodb.core.MongoTemplateTests.VersionedPerson;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexType;
import org.springframework.data.mongodb.core.index.GeospatialIndex;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperationsAdapter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
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

	@Before
	public void setUp() {

		StepVerifier
				.create(template.dropCollection("people") //
						.mergeWith(template.dropCollection("personX")) //
						.mergeWith(template.dropCollection("collection")) //
						.mergeWith(template.dropCollection(Person.class)) //
						.mergeWith(template.dropCollection(Venue.class)) //
						.mergeWith(template.dropCollection(PersonWithAList.class)) //
						.mergeWith(template.dropCollection(PersonWithIdPropertyOfTypeObjectId.class)) //
						.mergeWith(template.dropCollection(PersonWithVersionPropertyOfTypeInteger.class)) //
						.mergeWith(template.dropCollection(Sample.class))) //
				.verifyComplete();
	}

	@After
	public void cleanUp() {}

	@Test // DATAMONGO-1444
	public void insertSetsId() {

		PersonWithAList person = new PersonWithAList();
		assert person.getId() == null;

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertAllSetsId() {

		PersonWithAList person = new PersonWithAList();

		StepVerifier.create(template.insertAll(Collections.singleton(person))).expectNextCount(1).verifyComplete();

		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertCollectionSetsId() {

		PersonWithAList person = new PersonWithAList();

		StepVerifier.create(template.insert(Collections.singleton(person), PersonWithAList.class)).expectNextCount(1)
				.verifyComplete();

		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void saveSetsId() {

		PersonWithAList person = new PersonWithAList();
		assert person.getId() == null;

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertsSimpleEntityCorrectly() {

		Person person = new Person("Mark");
		person.setAge(35);
		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.find(new Query(where("_id").is(person.getId())), Person.class)) //
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
		StepVerifier.create(template.insert(person, "people")).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.find(new Query(where("_id").is(person.getId())), Person.class, "people")) //
				.expectNext(person) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		StepVerifier.create(template.insertAll(people)).expectNextCount(3).verifyComplete();

		StepVerifier.create(template.find(new Query().with(Sort.by("firstname")), Person.class)) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchWithSuppliedCollectionNameCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		StepVerifier.create(template.insert(people, "people")).expectNextCount(3).verifyComplete();

		StepVerifier.create(template.find(new Query().with(Sort.by("firstname")), Person.class, "people")) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertBatchWithSuppliedEntityTypeCorrectly() {

		List<Person> people = Arrays.asList(new Person("Dick", 22), new Person("Harry", 23), new Person("Tom", 21));

		StepVerifier.create(template.insert(people, Person.class)).expectNextCount(3).verifyComplete();

		StepVerifier.create(template.find(new Query().with(Sort.by("firstname")), Person.class)) //
				.expectNextSequence(people) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testAddingToList() {

		PersonWithAList person = createPersonWithAList("Sven", 22);
		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		Query query = new Query(where("id").is(person.getId()));

		StepVerifier.create(template.findOne(query, PersonWithAList.class)).consumeNextWith(actual -> {

			assertThat(actual.getWishList().size(), is(0));
		}).verifyComplete();

		person.addToWishList("please work!");

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findOne(query, PersonWithAList.class)).consumeNextWith(actual -> {

			assertThat(actual.getWishList().size(), is(1));
		}).verifyComplete();

		Friend friend = new Friend();
		person.setFirstName("Erik");
		person.setAge(21);

		person.addFriend(friend);
		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findOne(query, PersonWithAList.class)).consumeNextWith(actual -> {

			assertThat(actual.getWishList().size(), is(1));
			assertThat(actual.getFriends().size(), is(1));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void testFindOneWithSort() {

		PersonWithAList sven = createPersonWithAList("Sven", 22);
		PersonWithAList erik = createPersonWithAList("Erik", 21);
		PersonWithAList mark = createPersonWithAList("Mark", 40);

		StepVerifier.create(template.insertAll(Arrays.asList(sven, erik, mark))).expectNextCount(3).verifyComplete();

		// test query with a sort
		Query query = new Query(where("age").gt(10));
		query.with(Sort.by(Direction.DESC, "age"));

		StepVerifier.create(template.findOne(query, PersonWithAList.class)).consumeNextWith(actual -> {

			assertThat(actual.getFirstName(), is("Mark"));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void bogusUpdateDoesNotTriggerException() {

		ReactiveMongoTemplate mongoTemplate = new ReactiveMongoTemplate(factory);
		mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person oliver = new Person("Oliver2", 25);
		StepVerifier.create(template.insert(oliver)).expectNextCount(1).verifyComplete();

		Query q = new Query(where("BOGUS").gt(22));
		Update u = new Update().set("firstName", "Sven");

		StepVerifier.create(mongoTemplate.updateFirst(q, u, Person.class)).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateFirstByEntityTypeShouldUpdateObject() {

		Person person = new Person("Oliver2", 25);
		StepVerifier.create(template.insert(person) //
				.then(template.updateFirst(new Query(where("age").is(25)), new Update().set("firstName", "Sven"), Person.class)) //
				.flatMapMany(p -> template.find(new Query(where("age").is(25)), Person.class))).consumeNextWith(actual -> {

					assertThat(actual.getFirstName(), is(equalTo("Sven")));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateFirstByCollectionNameShouldUpdateObjects() {

		Person person = new Person("Oliver2", 25);
		StepVerifier
				.create(template.insert(person, "people") //
						.then(template.updateFirst(new Query(where("age").is(25)), new Update().set("firstName", "Sven"), "people")) //
						.flatMapMany(p -> template.find(new Query(where("age").is(25)), Person.class, "people")))
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName(), is(equalTo("Sven")));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateMultiByEntityTypeShouldUpdateObjects() {

		Query query = new Query(
				new Criteria().orOperator(where("firstName").is("Walter Jr"), Criteria.where("firstName").is("Walter")));

		StepVerifier
				.create(template
						.insertAll(Mono
								.just(Arrays.asList(new Person("Walter", 50), new Person("Skyler", 43), new Person("Walter Jr", 16)))) //
						.flatMap(a -> template.updateMulti(query, new Update().set("firstName", "Walt"), Person.class)) //
						.thenMany(template.find(new Query(where("firstName").is("Walt")), Person.class))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void updateMultiByCollectionNameShouldUpdateObject() {

		Query query = new Query(
				new Criteria().orOperator(where("firstName").is("Walter Jr"), Criteria.where("firstName").is("Walter")));

		List<Person> people = Arrays.asList(new Person("Walter", 50), //
				new Person("Skyler", 43), //
				new Person("Walter Jr", 16));

		Flux<Person> personFlux = template.insertAll(Mono.just(people), "people") //
				.collectList() //
				.flatMap(a -> template.updateMulti(query, new Update().set("firstName", "Walt"), Person.class, "people")) //
				.flatMapMany(p -> template.find(new Query(where("firstName").is("Walt")), Person.class, "people"));

		StepVerifier.create(personFlux) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForDuplicateIds() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.insert(person)).expectError(DataIntegrityViolationException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForUpdateWithInvalidPushOperator() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		Query query = new Query(where("firstName").is("Amol"));
		Update upd = new Update().push("age", 29);

		StepVerifier.create(template.updateFirst(query, upd, Person.class)) //
				.expectError(DataIntegrityViolationException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void rejectsDuplicateIdInInsertAll() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);

		ObjectId id = new ObjectId();
		Person person = new Person(id, "Amol");
		person.setAge(28);

		StepVerifier.create(template.insertAll(Arrays.asList(person, person))) //
				.expectError(DataIntegrityViolationException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void testFindAndUpdate() {

		StepVerifier
				.create(
						template.insertAll(Arrays.asList(new Person("Tom", 21), new Person("Dick", 22), new Person("Harry", 23)))) //
				.expectNextCount(3) //
				.verifyComplete();

		Query query = new Query(Criteria.where("firstName").is("Harry"));
		Update update = new Update().inc("age", 1);

		Person p = template.findAndModify(query, update, Person.class).block(); // return old
		assertThat(p.getFirstName(), is("Harry"));
		assertThat(p.getAge(), is(23));
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge(), is(24));

		p = template.findAndModify(query, update, Person.class, "person").block();
		assertThat(p.getAge(), is(24));
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge(), is(25));

		p = template.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Person.class).block();
		assertThat(p.getAge(), is(26));

		p = template.findAndModify(query, update, null, Person.class, "person").block();
		assertThat(p.getAge(), is(26));
		p = template.findOne(query, Person.class).block();
		assertThat(p.getAge(), is(27));

		Query query2 = new Query(Criteria.where("firstName").is("Mary"));
		p = template.findAndModify(query2, update, new FindAndModifyOptions().returnNew(true).upsert(true), Person.class)
				.block();
		assertThat(p.getFirstName(), is("Mary"));
		assertThat(p.getAge(), is(1));
	}

	@Test // DATAMONGO-1444
	public void testFindAllAndRemoveFullyReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		StepVerifier.create(template.insert(Arrays.asList(spring, data, mongodb), Sample.class)) //
				.expectNextCount(3) //
				.verifyComplete();

		Query qry = query(where("field").in("spring", "mongodb"));

		StepVerifier.create(template.findAllAndRemove(qry, Sample.class)).expectNextCount(2).verifyComplete();

		StepVerifier.create(template.findOne(new Query(), Sample.class)).expectNext(data).verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void testFindAllAndRemoveByCollectionReturnsAndRemovesDocuments() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		StepVerifier.create(template.insert(Arrays.asList(spring, data, mongodb), Sample.class)) //
				.expectNextCount(3) //
				.verifyComplete();

		Query qry = query(where("field").in("spring", "mongodb"));

		StepVerifier.create(template.findAllAndRemove(qry, "sample")).expectNextCount(2).verifyComplete();

		StepVerifier.create(template.findOne(new Query(), Sample.class)).expectNext(data).verifyComplete();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1774
	public void removeWithNullShouldThrowError() {
		template.remove((Object)null).subscribe();
	}

	@Test // DATAMONGO-1774
	public void removeWithEmptyMonoShouldDoNothing() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		StepVerifier.create(template.insert(Arrays.asList(spring, data, mongodb), Sample.class)) //
				.expectNextCount(3) //
				.verifyComplete();

		StepVerifier.create(template.remove(Mono.empty())).verifyComplete();
		StepVerifier.create(template.count(new Query(), Sample.class)).expectNext(3L).verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void removeWithMonoShouldDeleteElement() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		StepVerifier.create(template.insert(Arrays.asList(spring, data, mongodb), Sample.class)) //
				.expectNextCount(3) //
				.verifyComplete();

		StepVerifier.create(template.remove(Mono.just(spring))).expectNextCount(1).verifyComplete();
		StepVerifier.create(template.count(new Query(), Sample.class)).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1774
	public void removeWithMonoAndCollectionShouldDeleteElement() {

		Sample spring = new Sample("100", "spring");
		Sample data = new Sample("200", "data");
		Sample mongodb = new Sample("300", "mongodb");

		StepVerifier.create(template.insert(Arrays.asList(spring, data, mongodb), Sample.class)) //
				.expectNextCount(3) //
				.verifyComplete();

		StepVerifier.create(template.remove(Mono.just(spring), template.determineCollectionName(Sample.class)))
				.expectNextCount(1).verifyComplete();
		StepVerifier.create(template.count(new Query(), Sample.class)).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void optimisticLockingHandling() {

		// Init version
		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.age = 29;
		person.firstName = "Patryk";

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findAll(PersonWithVersionPropertyOfTypeInteger.class)).consumeNextWith(actual -> {

			assertThat(actual.version, is(0));
		}).verifyComplete();

		StepVerifier.create(template.findAll(PersonWithVersionPropertyOfTypeInteger.class).flatMap(p -> {

			// Version change
			person.firstName = "Patryk2";
			return template.save(person);
		})).expectNextCount(1).verifyComplete();

		assertThat(person.version, is(1));

		StepVerifier.create(template.findAll(PersonWithVersionPropertyOfTypeInteger.class)).consumeNextWith(actual -> {

			assertThat(actual.version, is(1));
		}).verifyComplete();

		// Optimistic lock exception
		person.version = 0;
		person.firstName = "Patryk3";

		StepVerifier.create(template.save(person)).expectError(OptimisticLockingFailureException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void doesNotFailOnVersionInitForUnversionedEntity() {

		Document dbObject = new Document();
		dbObject.put("firstName", "Oliver");

		StepVerifier
				.create(template.insert(dbObject, //
						template.determineCollectionName(PersonWithVersionPropertyOfTypeInteger.class))) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void removesObjectFromExplicitCollection() {

		String collectionName = "explicit";
		StepVerifier.create(template.remove(new Query(), collectionName)).expectNextCount(1).verifyComplete();

		PersonWithConvertedId person = new PersonWithConvertedId();
		person.name = "Dave";

		StepVerifier.create(template.save(person, collectionName)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findAll(PersonWithConvertedId.class, collectionName)).expectNextCount(1)
				.verifyComplete();

		StepVerifier.create(template.remove(person, collectionName)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findAll(PersonWithConvertedId.class, collectionName)).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void savesMapCorrectly() {

		Map<String, String> map = new HashMap<>();
		map.put("key", "value");

		StepVerifier.create(template.save(map, "maps")).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-1444, DATAMONGO-1730
	public void savesMongoPrimitiveObjectCorrectly() {

		StepVerifier.create(template.save(new Object(), "collection")) //
				.expectError(MappingException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void savesPlainDbObjectCorrectly() {

		Document dbObject = new Document("foo", "bar");

		StepVerifier.create(template.save(dbObject, "collection")).expectNextCount(1).verifyComplete();

		assertThat(dbObject.containsKey("_id"), is(true));
	}

	@Test(expected = MappingException.class) // DATAMONGO-1444, DATAMONGO-1730
	public void rejectsPlainObjectWithOutExplicitCollection() {

		Document dbObject = new Document("foo", "bar");

		StepVerifier.create(template.save(dbObject, "collection")).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findById(dbObject.get("_id"), Document.class)) //
				.expectError(MappingException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void readsPlainDbObjectById() {

		Document dbObject = new Document("foo", "bar");
		StepVerifier.create(template.save(dbObject, "collection")).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findById(dbObject.get("_id"), Document.class, "collection")) //
				.consumeNextWith(actual -> {

					assertThat(actual.get("foo"), is(dbObject.get("foo")));
					assertThat(actual.get("_id"), is(dbObject.get("_id")));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void geoNear() {

		List<Venue> venues = Arrays.asList(new Venue("Penn Station", -73.99408, 40.75057), //
				new Venue("10gen Office", -73.99171, 40.738868), //
				new Venue("Flatiron Building", -73.988135, 40.741404), //
				new Venue("Maplewood, NJ", -74.2713, 40.73137));

		StepVerifier.create(template.insertAll(venues)).expectNextCount(4).verifyComplete();

		IndexOperationsAdapter.blocking(template.indexOps(Venue.class))
				.ensureIndex(new GeospatialIndex("location").typed(GeoSpatialIndexType.GEO_2D));

		NearQuery geoFar = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(150, Metrics.KILOMETERS);

		StepVerifier.create(template.geoNear(geoFar, Venue.class)) //
				.expectNextCount(4) //
				.verifyComplete();

		NearQuery geoNear = NearQuery.near(-73, 40, Metrics.KILOMETERS).num(10).maxDistance(120, Metrics.KILOMETERS);

		StepVerifier.create(template.geoNear(geoNear, Venue.class)) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void writesPlainString() {

		StepVerifier.create(template.save("{ 'foo' : 'bar' }", "collection")) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void rejectsNonJsonStringForSave() {

		StepVerifier.create(template.save("Foobar!", "collection")) //
				.expectError(MappingException.class) //
				.verify();
	}

	@Test // DATAMONGO-1444
	public void initializesVersionOnInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		StepVerifier.create(template.insert(person)).expectNextCount(1).verifyComplete();

		assertThat(person.version, is(0));
	}

	@Test // DATAMONGO-1444
	public void initializesVersionOnBatchInsert() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		StepVerifier.create(template.insertAll(Collections.singleton(person))).expectNextCount(1).verifyComplete();

		assertThat(person.version, is(0));
	}

	@Test // DATAMONGO-1444
	public void queryCanBeNull() {

		StepVerifier.create(template.findAll(PersonWithIdPropertyOfTypeObjectId.class)).verifyComplete();

		StepVerifier.create(template.find(null, PersonWithIdPropertyOfTypeObjectId.class)).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void versionsObjectIntoDedicatedCollection() {

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		StepVerifier.create(template.save(person, "personX")).expectNextCount(1).verifyComplete();
		assertThat(person.version, is(0));

		StepVerifier.create(template.save(person, "personX")).expectNextCount(1).verifyComplete();
		assertThat(person.version, is(1));
	}

	@Test // DATAMONGO-1444
	public void correctlySetsLongVersionProperty() {

		PersonWithVersionPropertyOfTypeLong person = new PersonWithVersionPropertyOfTypeLong();
		person.firstName = "Dave";

		StepVerifier.create(template.save(person, "personX")).expectNextCount(1).verifyComplete();
		assertThat(person.version, is(0L));
	}

	@Test // DATAMONGO-1444
	public void throwsExceptionForIndexViolationIfConfigured() {

		ReactiveMongoTemplate template = new ReactiveMongoTemplate(factory);
		template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
		StepVerifier
				.create(template.indexOps(Person.class) //
						.ensureIndex(new Index().on("firstName", Direction.DESC).unique())) //
				.expectNextCount(1) //
				.verifyComplete();

		Person person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		person = new Person(new ObjectId(), "Amol");
		person.setAge(28);

		StepVerifier.create(template.save(person)).expectError(DataIntegrityViolationException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void preventsDuplicateInsert() {

		template.setWriteConcern(WriteConcern.MAJORITY);

		PersonWithVersionPropertyOfTypeInteger person = new PersonWithVersionPropertyOfTypeInteger();
		person.firstName = "Dave";

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();
		assertThat(person.version, is(0));

		person.version = null;
		StepVerifier.create(template.save(person)).expectError(DuplicateKeyException.class).verify();
	}

	@Test // DATAMONGO-1444
	public void countAndFindWithoutTypeInformation() {

		Person person = new Person();
		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		Query query = query(where("_id").is(person.getId()));
		String collectionName = template.getCollectionName(Person.class);

		StepVerifier.create(template.find(query, HashMap.class, collectionName)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.count(query, collectionName)).expectNext(1L).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void nullsPropertiesForVersionObjectUpdates() {

		VersionedPerson person = new VersionedPerson();
		person.firstname = "Dave";
		person.lastname = "Matthews";

		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		assertThat(person.id, is(notNullValue()));

		person.lastname = null;
		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findOne(query(where("id").is(person.id)), VersionedPerson.class)) //
				.consumeNextWith(actual -> {

					assertThat(actual.lastname, is(nullValue()));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void nullsValuesForUpdatesOfUnversionedEntity() {

		Person person = new Person("Dave");
		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		person.setFirstName(null);
		StepVerifier.create(template.save(person)).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findOne(query(where("id").is(person.getId())), Person.class)) //
				.consumeNextWith(actual -> {

					assertThat(actual.getFirstName(), is(nullValue()));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void savesJsonStringCorrectly() {

		Document dbObject = new Document().append("first", "first").append("second", "second");

		StepVerifier.create(template.save(dbObject, "collection")).expectNextCount(1).verifyComplete();

		StepVerifier.create(template.findAll(Document.class, "collection")) //
				.consumeNextWith(actual -> {

					assertThat(actual.containsKey("first"), is(true));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void executesExistsCorrectly() {

		Sample sample = new Sample();
		StepVerifier.create(template.save(sample)).expectNextCount(1).verifyComplete();

		Query query = query(where("id").is(sample.id));

		StepVerifier.create(template.exists(query, Sample.class)).expectNext(true).verifyComplete();

		StepVerifier.create(template.exists(query(where("_id").is(sample.id)), template.getCollectionName(Sample.class)))
				.expectNext(true).verifyComplete();

		StepVerifier.create(template.exists(query, Sample.class, template.getCollectionName(Sample.class))).expectNext(true)
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void tailStreamsData() throws InterruptedException {

		StepVerifier.create(template.dropCollection("capped")
				.then(template.createCollection("capped", //
						CollectionOptions.empty().size(1000).maxDocuments(10).capped()))
				.then(template.insert(new Document("random", Math.random()).append("key", "value"), //
						"capped")))
				.expectNextCount(1).verifyComplete();

		BlockingQueue<Document> documents = new LinkedBlockingQueue<>(1000);

		Flux<Document> capped = template.tail(null, Document.class, "capped");

		Disposable disposable = capped.doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));
		assertThat(documents.isEmpty(), is(true));

		disposable.dispose();
	}

	@Test // DATAMONGO-1444
	public void tailStreamsDataUntilCancellation() throws InterruptedException {

		StepVerifier.create(template.dropCollection("capped")
				.then(template.createCollection("capped", //
						CollectionOptions.empty().size(1000).maxDocuments(10).capped()))
				.then(template.insert(new Document("random", Math.random()).append("key", "value"), //
						"capped")))
				.expectNextCount(1).verifyComplete();

		BlockingQueue<Document> documents = new LinkedBlockingQueue<>(1000);

		Flux<Document> capped = template.tail(null, Document.class, "capped");

		Disposable disposable = capped.doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));
		assertThat(documents.isEmpty(), is(true));

		StepVerifier.create(template.insert(new Document("random", Math.random()).append("key", "value"), "capped")) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));

		disposable.dispose();

		StepVerifier.create(template.insert(new Document("random", Math.random()).append("key", "value"), "capped")) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(documents.poll(1, TimeUnit.SECONDS), is(nullValue()));
	}

	private PersonWithAList createPersonWithAList(String firstname, int age) {

		PersonWithAList p = new PersonWithAList();
		p.setFirstName(firstname);
		p.setAge(age);

		return p;
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

}
