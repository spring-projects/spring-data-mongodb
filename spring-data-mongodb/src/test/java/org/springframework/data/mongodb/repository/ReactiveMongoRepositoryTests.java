/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.Sort.Direction.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;
import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Test for {@link ReactiveMongoRepository} query methods.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
public class ReactiveMongoRepositoryTests {

	static @Client MongoClient mongoClient;

	@Autowired ReactiveMongoTemplate template;

	@Autowired ReactivePersonRepository repository;
	@Autowired ReactiveContactRepository contactRepository;
	@Autowired ReactiveCappedCollectionRepository cappedRepository;

	Person dave, oliver, carter, boyd, stefan, leroi, alicia;
	QPerson person = QPerson.person;

	@Configuration
	static class Config extends AbstractReactiveMongoConfiguration {

		@Bean
		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "reactive";
		}

		@Bean
		ReactiveMongoRepositoryFactory factory(ReactiveMongoOperations template, BeanFactory beanFactory) {

			ReactiveMongoRepositoryFactory factory = new ReactiveMongoRepositoryFactory(template);
			factory.setRepositoryBaseClass(SimpleReactiveMongoRepository.class);
			factory.setBeanClassLoader(beanFactory.getClass().getClassLoader());
			factory.setBeanFactory(beanFactory);
			factory.setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);

			return factory;
		}

		@Bean
		ReactivePersonRepository reactivePersonRepository(ReactiveMongoRepositoryFactory factory) {
			return factory.getRepository(ReactivePersonRepository.class);
		}

		@Bean
		ReactiveContactRepository reactiveContactRepository(ReactiveMongoRepositoryFactory factory) {
			return factory.getRepository(ReactiveContactRepository.class);
		}

		@Bean
		ReactiveCappedCollectionRepository reactiveCappedCollectionRepository(ReactiveMongoRepositoryFactory factory) {
			return factory.getRepository(ReactiveCappedCollectionRepository.class);
		}

		@Override
		protected boolean autoIndexCreation() {
			return true;
		}
	}

	@BeforeAll
	public static void cleanDb() {

		MongoTestUtils.createOrReplaceCollectionNow("reactive", "person", mongoClient);
		MongoTestUtils.createOrReplaceCollectionNow("reactive", "capped", mongoClient);
	}

	@BeforeEach
	public void setUp() throws Exception {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave = new Person("Dave", "Matthews", 42);
		oliver = new Person("Oliver August", "Matthews", 4);
		carter = new Person("Carter", "Beauford", 49);
		carter.setSkills(Arrays.asList("Drums", "percussion", "vocals"));
		Thread.sleep(10);
		boyd = new Person("Boyd", "Tinsley", 45);
		boyd.setSkills(Arrays.asList("Violin", "Electric Violin", "Viola", "Mandolin", "Vocals", "Guitar"));
		stefan = new Person("Stefan", "Lessard", 34);
		leroi = new Person("Leroi", "Moore", 41);

		alicia = new Person("Alicia", "Keys", 30, Sex.FEMALE);

		repository.saveAll(Arrays.asList(oliver, carter, boyd, stefan, leroi, alicia, dave)).as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByLastName() {
		repository.findByLastname(dave.getLastname()).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneByLastName() {
		repository.findOneByLastname(carter.getLastname()).as(StepVerifier::create).expectNext(carter).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneByPublisherOfLastName() {
		repository.findByLastname(Mono.just(carter.getLastname())).as(StepVerifier::create).expectNext(carter)
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameIn() {
		repository.findByLastnameIn(Flux.just(carter.getLastname(), dave.getLastname())).as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameInAndAgeGreater() {

		repository.findByLastnameInAndAgeGreaterThan(Flux.just(carter.getLastname(), dave.getLastname()), 41)
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindUsingPublishersInStringQuery() {

		repository.findStringQuery(Flux.just("Beauford", "Matthews"), Mono.just(41)).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByLastNameAndSort() {

		repository.findByLastname("Matthews", Sort.by(ASC, "age")).as(StepVerifier::create) //
				.expectNext(oliver, dave) //
				.verifyComplete();

		repository.findByLastname("Matthews", Sort.by(DESC, "age")).as(StepVerifier::create) //
				.expectNext(dave, oliver) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldUseTailableCursor() throws Exception {

		template.dropCollection(Capped.class) //
				.then(template.createCollection(Capped.class, //
						CollectionOptions.empty().size(1000).maxDocuments(100).capped()))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.insert(new Capped("value", Math.random())).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<Capped> documents = new LinkedBlockingDeque<>(100);

		Disposable disposable = cappedRepository.findByKey("value").doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS)).isNotNull();

		template.insert(new Capped("value", Math.random())).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		assertThat(documents.poll(5, TimeUnit.SECONDS)).isNotNull();
		assertThat(documents).isEmpty();

		disposable.dispose();
	}

	@Test // DATAMONGO-1444
	public void shouldUseTailableCursorWithProjection() throws Exception {

		template.dropCollection(Capped.class) //
				.then(template.createCollection(Capped.class, //
						CollectionOptions.empty().size(1000).maxDocuments(100).capped()))
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		template.insert(new Capped("value", Math.random())).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		BlockingQueue<CappedProjection> documents = new LinkedBlockingDeque<>(100);

		Disposable disposable = cappedRepository.findProjectionByKey("value").doOnNext(documents::add).subscribe();

		CappedProjection projection1 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection1).isNotNull();
		assertThat(projection1.getRandom()).isNotEqualTo(0);

		template.insert(new Capped("value", Math.random())).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		CappedProjection projection2 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection2).isNotNull();
		assertThat(projection2.getRandom()).isNotEqualTo(0);

		assertThat(documents).isEmpty();

		disposable.dispose();
	}

	@Test // DATAMONGO-2080
	public void shouldUseTailableCursorWithDtoProjection() {

		template.dropCollection(Capped.class) //
				.then(template.createCollection(Capped.class, //
						CollectionOptions.empty().size(1000).maxDocuments(100).capped())) //
				.as(StepVerifier::create).expectNextCount(1) //
				.verifyComplete();

		template.insert(new Capped("value", Math.random())).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		cappedRepository.findDtoProjectionByKey("value").as(StepVerifier::create).expectNextCount(1).thenCancel().verify();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170)).as(StepVerifier::create) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByPageableLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170), //
				PageRequest.of(0, 10)).as(StepVerifier::create) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleGeoresultByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		repository.findByLocationNear(new Point(-73.99, 40.73), //
				new Distance(2000, Metrics.KILOMETERS)).as(StepVerifier::create).consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(1, offset(1d));
					assertThat(actual.getContent()).isEqualTo(dave);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeoplePageableGeoresultByLocationWithinBox() throws InterruptedException {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		// Allow for index creation
		Thread.sleep(500);

		repository.findByLocationNear(new Point(-73.99, 40.73), //
				new Distance(2000, Metrics.KILOMETERS), //
				PageRequest.of(0, 10)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(1, offset(1d));
					assertThat(actual.getContent()).isEqualTo(dave);
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByLocationWithinBox() throws InterruptedException {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		// Allow for index creation
		Thread.sleep(500);

		repository.findPersonByLocationNear(new Point(-73.99, 40.73), //
				new Distance(2000, Metrics.KILOMETERS)).as(StepVerifier::create) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1865
	public void shouldErrorOnFindOneWithNonUniqueResult() {
		repository.findOneByLastname(dave.getLastname()).as(StepVerifier::create)
				.expectError(IncorrectResultSizeDataAccessException.class).verify();
	}

	@Test // DATAMONGO-1865
	public void shouldReturnFirstFindFirstWithMoreResults() {
		repository.findFirstByLastname(dave.getLastname()).as(StepVerifier::create).expectNextCount(1).verifyComplete();
	}

	@Test // DATAMONGO-2030
	public void shouldReturnExistsBy() {
		repository.existsByLastname(dave.getLastname()).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1979
	public void findAppliesAnnotatedSort() {

		repository.findByAgeGreaterThan(40).collectList().as(StepVerifier::create).consumeNextWith(result -> {
			assertThat(result).containsSequence(carter, boyd, dave, leroi);
		}).verifyComplete();
	}

	@Test // DATAMONGO-1979
	public void findWithSortOverwritesAnnotatedSort() {

		repository.findByAgeGreaterThan(40, Sort.by(Direction.ASC, "age")).collectList().as(StepVerifier::create)
				.consumeNextWith(result -> {
					assertThat(result).containsSequence(leroi, dave, boyd, carter);
				}).verifyComplete();
	}

	@Test // DATAMONGO-2181
	public void considersRepositoryCollectionName() {

		repository.deleteAll() //
				.as(StepVerifier::create) //
				.verifyComplete();

		contactRepository.deleteAll() //
				.as(StepVerifier::create) //
				.verifyComplete();

		leroi.id = null;
		boyd.id = null;
		contactRepository.saveAll(Arrays.asList(leroi, boyd)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		contactRepository.count() //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2182
	public void shouldFindPersonsWhenUsingQueryDslPerdicatedOnIdProperty() {

		repository.findAll(person.id.in(Arrays.asList(dave.id, carter.id))) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual).containsExactlyInAnyOrder(dave, carter);
				}).verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void findListOfSingleValue() {

		repository.findAllLastnames() //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual) //
							.contains("Lessard") //
							.contains("Keys") //
							.contains("Tinsley") //
							.contains("Beauford") //
							.contains("Moore") //
							.contains("Matthews");
				}).verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithPlaceholderValue() {

		repository.groupByLastnameAnd("firstname") //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual) //
							.contains(new PersonAggregate("Lessard", "Stefan")) //
							.contains(new PersonAggregate("Keys", "Alicia")) //
							.contains(new PersonAggregate("Tinsley", "Boyd")) //
							.contains(new PersonAggregate("Beauford", "Carter")) //
							.contains(new PersonAggregate("Moore", "Leroi")) //
							.contains(new PersonAggregate("Matthews", Arrays.asList("Dave", "Oliver August")));
				}).verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithSort() {

		repository.groupByLastnameAnd("firstname", Sort.by("lastname")) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual) //
							.containsSequence( //
									new PersonAggregate("Beauford", "Carter"), //
									new PersonAggregate("Keys", "Alicia"), //
									new PersonAggregate("Lessard", "Stefan"), //
									new PersonAggregate("Matthews", Arrays.asList("Dave", "Oliver August")), //
									new PersonAggregate("Moore", "Leroi"), //
									new PersonAggregate("Tinsley", "Boyd"));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithPageable() {

		repository.groupByLastnameAnd("firstname", PageRequest.of(1, 2, Sort.by("lastname"))) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual) //
							.containsExactly( //
									new PersonAggregate("Lessard", "Stefan"), //
									new PersonAggregate("Matthews", Arrays.asList("Dave", "Oliver August")));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithSingleSimpleResult() {

		repository.sumAge() //
				.as(StepVerifier::create) //
				.expectNext(245L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithAggregationResultAsReturnType() {

		repository.sumAgeAndReturnRawResult() //
				.as(StepVerifier::create) //
				.expectNext(new org.bson.Document("_id", null).append("total", 245)) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithAggregationResultAsReturnTypeAndProjection() {

		repository.sumAgeAndReturnSumWrapper() //
				.as(StepVerifier::create) //
				.expectNext(new SumAge(245L)) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2374
	public void findsWithNativeProjection() {

		repository.findDocumentById(dave.getId()) //
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {
					assertThat(it).containsEntry("firstname", dave.getFirstname()).containsEntry("lastname", dave.getLastname());
				}).verifyComplete();
	}

	@Test // DATAMONGO-2153
	public void annotatedAggregationWithAggregationResultAsMap() {

		repository.sumAgeAndReturnSumAsMap() //
				.as(StepVerifier::create) //
				.consumeNextWith(it -> {
					assertThat(it).isInstanceOf(Map.class);
				}).verifyComplete();
	}

	@Test // DATAMONGO-2403
	public void annotatedAggregationExtractingSimpleValueIsEmptyForEmptyDocument() {

		Person p = new Person("project-on-lastanme", null);
		repository.save(p).then().as(StepVerifier::create).verifyComplete();

		repository.projectToLastnameAndRemoveId(p.getFirstname()) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2403
	public void annotatedAggregationSkipsEmptyDocumentsWhenExtractingSimpleValue() {

		String firstname = "project-on-lastanme";

		Person p1 = new Person(firstname, null);
		p1.setEmail("p1@example.com");
		Person p2 = new Person(firstname, "lastname");
		p2.setEmail("p2@example.com");
		Person p3 = new Person(firstname, null);
		p3.setEmail("p3@example.com");

		repository.saveAll(Arrays.asList(p1, p2, p3)).then().as(StepVerifier::create).verifyComplete();

		repository.projectToLastnameAndRemoveId(firstname) //
				.as(StepVerifier::create) //
				.expectNext("lastname").verifyComplete();
	}

	@Test // DATAMONGO-2406
	public void deleteByShouldHandleVoidResultTypeCorrectly() {

		repository.deleteByLastname(dave.getLastname()) //
				.as(StepVerifier::create) //
				.verifyComplete();

		template.find(query(where("lastname").is(dave.getLastname())), Person.class) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1997
	public void deleteByShouldAllowDeletedCountAsResult() {

		repository.deleteCountByLastname(dave.getLastname()) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1997
	public void deleteByShouldAllowSingleDocumentRemovalCorrectly() {

		repository.deleteSinglePersonByLastname(carter.getLastname()) //
				.as(StepVerifier::create) //
				.expectNext(carter) //
				.verifyComplete();

		repository.deleteSinglePersonByLastname("dorfuaeB") //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	interface ReactivePersonRepository
			extends ReactiveMongoRepository<Person, String>, ReactiveQuerydslPredicateExecutor<Person> {

		Flux<Person> findByLastname(String lastname);

		Mono<Person> findOneByLastname(String lastname);

		Mono<DtoProjection> findOneProjectedByLastname(String lastname);

		Mono<Person> findByLastname(Publisher<String> lastname);

		Flux<Person> findByLastnameIn(Publisher<String> lastname);

		Flux<Person> findByLastname(String lastname, Sort sort);

		Flux<Person> findByLastnameInAndAgeGreaterThan(Flux<String> lastname, int age);

		@Query("{ lastname: { $in: ?0 }, age: { $gt : ?1 } }")
		Flux<Person> findStringQuery(Flux<String> lastname, Mono<Integer> age);

		Flux<Person> findByLocationWithin(Circle circle);

		Flux<Person> findByLocationWithin(Circle circle, Pageable pageable);

		Flux<GeoResult<Person>> findByLocationNear(Point point, Distance maxDistance);

		Flux<GeoResult<Person>> findByLocationNear(Point point, Distance maxDistance, Pageable pageable);

		Flux<Person> findPersonByLocationNear(Point point, Distance maxDistance);

		Mono<Boolean> existsByLastname(String lastname);

		Mono<Person> findFirstByLastname(String lastname);

		@Query(sort = "{ age : -1 }")
		Flux<Person> findByAgeGreaterThan(int age);

		@Query(sort = "{ age : -1 }")
		Flux<Person> findByAgeGreaterThan(int age, Sort sort);

		@Aggregation("{ '$project': { '_id' : '$lastname' } }")
		Flux<String> findAllLastnames();

		@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
		Flux<PersonAggregate> groupByLastnameAnd(String property);

		@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
		Flux<PersonAggregate> groupByLastnameAnd(String property, Sort sort);

		@Aggregation("{ '$group': { '_id' : '$lastname', names : { $addToSet : '$?0' } } }")
		Flux<PersonAggregate> groupByLastnameAnd(String property, Pageable page);

		@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
		Mono<Long> sumAge();

		@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
		Mono<org.bson.Document> sumAgeAndReturnRawResult();

		@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
		Mono<SumAge> sumAgeAndReturnSumWrapper();

		@Aggregation(pipeline = "{ '$group' : { '_id' : null, 'total' : { $sum: '$age' } } }")
		Mono<Map> sumAgeAndReturnSumAsMap();

		@Aggregation(
				pipeline = { "{ '$match' : { 'firstname' : '?0' } }", "{ '$project' : { '_id' : 0, 'lastname' : 1 } }" })
		Mono<String> projectToLastnameAndRemoveId(String firstname);

		@Query(value = "{_id:?0}")
		Mono<org.bson.Document> findDocumentById(String id);

		Mono<Void> deleteByLastname(String lastname);

		Mono<Long> deleteCountByLastname(String lastname);

		Mono<Person> deleteSinglePersonByLastname(String lastname);
	}

	interface ReactiveContactRepository extends ReactiveMongoRepository<Contact, String> {}

	interface ReactiveCappedCollectionRepository extends Repository<Capped, String> {

		@Tailable
		Flux<Capped> findByKey(String key);

		@Tailable
		Flux<CappedProjection> findProjectionByKey(String key);

		@Tailable
		Flux<DtoProjection> findDtoProjectionByKey(String key);
	}

	@Document
	@NoArgsConstructor
	static class Capped {

		String id;
		String key;
		double random;

		public Capped(String key, double random) {
			this.key = key;
			this.random = random;
		}
	}

	interface CappedProjection {
		double getRandom();
	}

	@Data
	static class DtoProjection {
		String id;
		double unknown;
	}
}
