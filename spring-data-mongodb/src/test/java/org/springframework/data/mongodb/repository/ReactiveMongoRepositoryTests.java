/*
 * Copyright 2016 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
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

import lombok.NoArgsConstructor;
import org.springframework.util.ClassUtils;
import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

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
	private ClassLoader classLoader;
	private BeanFactory beanFactory;
	private ReactivePersonRepository repository;
	private ReactiveCappedCollectionRepository cappedRepository;

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

		repository.deleteAll().block();

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

		TestSubscriber<Person> subscriber = TestSubscriber.create();
		repository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia)).subscribe(subscriber);

		subscriber.await().assertComplete().assertNoError();
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindByLastName() {

		List<Person> list = repository.findByLastname("Matthews").collectList().block();

		assertThat(list, hasSize(2));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindMonoOfPage() {

		Mono<Page<Person>> pageMono = repository.findMonoPageByLastname("Matthews", new PageRequest(0, 1));

		Page<Person> persons = pageMono.block();

		assertThat(persons.getContent(), hasSize(1));
		assertThat(persons.getTotalPages(), is(2));

		pageMono = repository.findMonoPageByLastname("Matthews", new PageRequest(0, 100));

		persons = pageMono.block();

		assertThat(persons.getContent(), hasSize(2));
		assertThat(persons.getTotalPages(), is(1));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindMonoOfSlice() {

		Mono<Slice<Person>> pageMono = repository.findMonoSliceByLastname("Matthews", new PageRequest(0, 1));

		Slice<Person> persons = pageMono.block();

		assertThat(persons.getContent(), hasSize(1));
		assertThat(persons.hasNext(), is(true));

		pageMono = repository.findMonoSliceByLastname("Matthews", new PageRequest(0, 100));

		persons = pageMono.block();

		assertThat(persons.getContent(), hasSize(2));
		assertThat(persons.hasNext(), is(false));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindOneByLastName() {

		Person carter = repository.findOneByLastname("Beauford").block();

		assertThat(carter.getFirstname(), is(equalTo("Carter")));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindOneByPublisherOfLastName() {

		Person carter = repository.findByLastname(Mono.just("Beauford")).block();

		assertThat(carter.getFirstname(), is(equalTo("Carter")));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindByPublisherOfLastNameIn() {

		List<Person> persons = repository.findByLastnameIn(Flux.just("Beauford", "Matthews")).collectList().block();

		assertThat(persons, hasItems(carter, dave, oliver));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindByPublisherOfLastNameInAndAgeGreater() {

		List<Person> persons = repository.findByLastnameInAndAgeGreaterThan(Flux.just("Beauford", "Matthews"), 41)
				.collectList().block();

		assertThat(persons, hasItems(carter, dave));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindUsingPublishersInStringQuery() {

		List<Person> persons = repository.findStringQuery(Flux.just("Beauford", "Matthews"), Mono.just(41)).collectList()
				.block();

		assertThat(persons, hasItems(carter, dave));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldFindByLastNameAndSort() {

		List<Person> persons = repository.findByLastname("Matthews", new Sort(new Order(ASC, "age"))).collectList().block();
		assertThat(persons, contains(oliver, dave));

		persons = repository.findByLastname("Matthews", new Sort(new Order(DESC, "age"))).collectList().block();
		assertThat(persons, contains(dave, oliver));
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldUseInfiniteStream() throws Exception {

		template.dropCollection(Capped.class).block();
		template.createCollection(Capped.class, new CollectionOptions(1000, 100, true)).block();
		template.insert(new Capped("value", Math.random())).block();

		BlockingQueue<Capped> documents = new LinkedBlockingDeque<>(100);

		Cancellation cancellation = cappedRepository.findByKey("value").doOnNext(documents::add).subscribe();

		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));

		template.insert(new Capped("value", Math.random())).block();
		assertThat(documents.poll(5, TimeUnit.SECONDS), is(notNullValue()));
		assertThat(documents.isEmpty(), is(true));

		cancellation.dispose();
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void shouldUseInfiniteStreamWithProjection() throws Exception {

		template.dropCollection(Capped.class).block();
		template.createCollection(Capped.class, new CollectionOptions(1000, 100, true)).block();
		template.insert(new Capped("value", Math.random())).block();

		BlockingQueue<CappedProjection> documents = new LinkedBlockingDeque<>(100);

		Cancellation cancellation = cappedRepository.findProjectionByKey("value").doOnNext(documents::add).subscribe();

		CappedProjection projection1 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection1, is(notNullValue()));
		assertThat(projection1.getRandom(), is(not(0)));

		template.insert(new Capped("value", Math.random())).block();

		CappedProjection projection2 = documents.poll(5, TimeUnit.SECONDS);
		assertThat(projection2, is(notNullValue()));
		assertThat(projection2.getRandom(), is(not(0)));

		assertThat(documents.isEmpty(), is(true));

		cancellation.dispose();
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void findsPeopleByLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).block();

		repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170)) //
				.subscribeWith(TestSubscriber.create()) //
				.awaitAndAssertNextValues(dave);
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void findsPeopleByPageableLocationWithinCircle() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).block();

		repository.findByLocationWithin(new Circle(-78.99171, 45.738868, 170), new PageRequest(0, 10)) //
				.subscribeWith(TestSubscriber.create()) //
				.awaitAndAssertNextValues(dave);
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void findsPeopleGeoresultByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).block();

		repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000, Metrics.KILOMETERS)) //
				.subscribeWith(TestSubscriber.create()) //
				.awaitAndAssertNextValuesWith(personGeoResult -> {

					assertThat(personGeoResult.getDistance().getValue(), is(closeTo(1, 1)));
					assertThat(personGeoResult.getContent(), is(equalTo(dave)));
				});
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void findsPeoplePageableGeoresultByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).block();

		repository.findByLocationNear(new Point(-73.99, 40.73), new Distance(2000, Metrics.KILOMETERS), new PageRequest(0, 10)) //
				.subscribeWith(TestSubscriber.create()) //
				.awaitAndAssertNextValuesWith(personGeoResult -> {

					assertThat(personGeoResult.getDistance().getValue(), is(closeTo(1, 1)));
					assertThat(personGeoResult.getContent(), is(equalTo(dave)));
				});
	}

	/**
	 * @see DATAMONGO-1444
	 */
	@Test
	public void findsPeopleByLocationWithinBox() {

		Point point = new Point(-73.99171, 40.738868);
		dave.setLocation(point);
		repository.save(dave).block();

		repository.findPersonByLocationNear(new Point(-73.99, 40.73), new Distance(2000, Metrics.KILOMETERS)) //
				.subscribeWith(TestSubscriber.create()) //
				.awaitAndAssertNextValues(dave);
	}

	interface ReactivePersonRepository extends ReactiveMongoRepository<Person, String> {

		Flux<Person> findByLastname(String lastname);

		Mono<Person> findOneByLastname(String lastname);

		Mono<Page<Person>> findMonoPageByLastname(String lastname, Pageable pageRequest);

		Mono<Slice<Person>> findMonoSliceByLastname(String lastname, Pageable pageRequest);

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
