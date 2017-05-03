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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.springframework.data.domain.Sort.Direction.*;

import lombok.NoArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.Person.Sex;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;

/**
 * Test for {@link ReactiveMongoRepository} query methods.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class ReactiveMongoRepositoryTests implements BeanClassLoaderAware, BeanFactoryAware {

	@Autowired ReactiveMongoTemplate template;

	ReactiveMongoRepositoryFactory factory;
	ClassLoader classLoader;
	BeanFactory beanFactory;
	ReactivePersonRepository repository;
	ReactiveCappedCollectionRepository cappedRepository;

	Person dave, oliver, carter, boyd, stefan, leroi, alicia;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Before
	public void setUp() throws Exception {

		factory = new ReactiveMongoRepositoryFactory(template);
		factory.setRepositoryBaseClass(SimpleReactiveMongoRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(DefaultEvaluationContextProvider.INSTANCE);

		repository = factory.getRepository(ReactivePersonRepository.class);
		cappedRepository = factory.getRepository(ReactiveCappedCollectionRepository.class);

		StepVerifier.create(repository.deleteAll()).verifyComplete();

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

		StepVerifier.create(repository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia))) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByLastName() {
		StepVerifier.create(repository.findByLastname(dave.getLastname())).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneByLastName() {
		StepVerifier.create(repository.findOneByLastname(carter.getLastname())).expectNext(carter);
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneByPublisherOfLastName() {
		StepVerifier.create(repository.findByLastname(Mono.just(carter.getLastname()))).expectNext(carter);
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameIn() {
		StepVerifier.create(repository.findByLastnameIn(Flux.just(carter.getLastname(), dave.getLastname()))) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameInAndAgeGreater() {

		StepVerifier
				.create(repository.findByLastnameInAndAgeGreaterThan(Flux.just(carter.getLastname(), dave.getLastname()), 41)) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindUsingPublishersInStringQuery() {

		StepVerifier.create(repository.findStringQuery(Flux.just("Beauford", "Matthews"), Mono.just(41))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByLastNameAndSort() {
		StepVerifier.create(repository.findByLastname("Matthews", Sort.by(ASC, "age"))) //
				.expectNext(oliver, dave) //
				.verifyComplete();

		StepVerifier.create(repository.findByLastname("Matthews", Sort.by(DESC, "age"))) //
				.expectNext(dave, oliver) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldUseInfiniteStream() throws Exception {

		StepVerifier
				.create(template.dropCollection(Capped.class) //
						.then(template.createCollection(Capped.class, //
								new CollectionOptions(1000, 100, true)))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(template.insert(new Capped("value", Math.random()))).expectNextCount(1).verifyComplete();

		BlockingQueue<Capped> documents = new LinkedBlockingDeque<>(100);

		Disposable disposable = cappedRepository.findByKey("value").doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));

		StepVerifier.create(template.insert(new Capped("value", Math.random()))).expectNextCount(1).verifyComplete();
		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));
		assertThat(documents.isEmpty(), is(true));

		disposable.dispose();
	}

	@Test // DATAMONGO-1444
	public void shouldUseInfiniteStreamWithProjection() throws Exception {

		StepVerifier
				.create(template.dropCollection(Capped.class) //
						.then(template.createCollection(Capped.class, //
								new CollectionOptions(1000, 100, true)))) //
				.expectNextCount(1) //
				.verifyComplete();

		StepVerifier.create(template.insert(new Capped("value", Math.random()))).expectNextCount(1).verifyComplete();

		BlockingQueue<CappedProjection> documents = new LinkedBlockingDeque<>(100);

		Disposable disposable = cappedRepository.findProjectionByKey("value").doOnNext(documents::add).subscribe();

		CappedProjection projection1 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection1, is(notNullValue()));
		assertThat(projection1.getRandom(), is(not(0)));

		StepVerifier.create(template.insert(new Capped("value", Math.random()))).expectNextCount(1).verifyComplete();

		CappedProjection projection2 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection2, is(notNullValue()));
		assertThat(projection2.getRandom(), is(not(0)));

		assertThat(documents.isEmpty(), is(true));

		disposable.dispose();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier.create(repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170))) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByPageableLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier
				.create(repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170), //
						PageRequest.of(0, 10))) //
				.expectNext(dave) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleGeoresultByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier.create(repository.findByLocationNear(new Point(-73.99, 40.73), //
				new Distance(2000, Metrics.KILOMETERS)) //
		).consumeNextWith(actual -> {

			assertThat(actual.getDistance().getValue(), is(closeTo(1, 1)));
			assertThat(actual.getContent(), is(equalTo(dave)));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeoplePageableGeoresultByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier
				.create(repository.findByLocationNear(new Point(-73.99, 40.73), //
						new Distance(2000, Metrics.KILOMETERS), //
						PageRequest.of(0, 10))) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue(), is(closeTo(1, 1)));
					assertThat(actual.getContent(), is(equalTo(dave)));
				}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findsPeopleByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		StepVerifier.create(repository.save(dave)).expectNextCount(1).verifyComplete();

		StepVerifier
				.create(repository.findPersonByLocationNear(new Point(-73.99, 40.73), //
						new Distance(2000, Metrics.KILOMETERS))) //
				.expectNext(dave) //
				.verifyComplete();
	}

	interface ReactivePersonRepository extends ReactiveMongoRepository<Person, String> {

		Flux<Person> findByLastname(String lastname);

		Mono<Person> findOneByLastname(String lastname);

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
	}

	interface ReactiveCappedCollectionRepository extends Repository<Capped, String> {

		@InfiniteStream
		Flux<Capped> findByKey(String key);

		@InfiniteStream
		Flux<CappedProjection> findProjectionByKey(String key);
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
}
