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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.repository.RepositoryDefinition;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.repository.reactive.RxJava1SortingRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;
import rx.Observable;
import rx.Single;

/**
 * Test for {@link ReactiveMongoRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ConvertingReactiveMongoRepositoryTests.Config.class)
public class ConvertingReactiveMongoRepositoryTests {

	@EnableReactiveMongoRepositories(includeFilters = @Filter(value = Repository.class), considerNestedRepositories = true)
	@ImportResource("classpath:reactive-infrastructure.xml")
	static class Config {}

	@Autowired ReactiveMongoTemplate template;
	@Autowired MixedReactivePersonRepostitory reactiveRepository;
	@Autowired ReactivePersonRepostitory reactivePersonRepostitory;
	@Autowired RxJavaPersonRepostitory rxJavaPersonRepostitory;

	ReactivePerson dave, oliver, carter, boyd, stefan, leroi, alicia;

	@Before
	public void setUp() throws Exception {

		reactiveRepository.deleteAll().block();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);

		TestSubscriber<ReactivePerson> subscriber = TestSubscriber.create();
		reactiveRepository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia)).subscribe(subscriber);

		subscriber.await().assertComplete().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void reactiveStreamsMethodsShouldWork() throws Exception {

		TestSubscriber<Boolean> subscriber = TestSubscriber.subscribe(reactivePersonRepostitory.exists(dave.getId()));

		subscriber.awaitAndAssertNextValueCount(1).assertValues(true);
	}

	@Test // DATAMONGO-1444
	public void reactiveStreamsQueryMethodsShouldWork() throws Exception {

		TestSubscriber<ReactivePerson> subscriber = TestSubscriber
				.subscribe(reactivePersonRepostitory.findByLastname(boyd.getLastname()));

		subscriber.awaitAndAssertNextValueCount(1).assertValues(boyd);
	}

	@Test // DATAMONGO-1444
	public void simpleRxJavaMethodsShouldWork() throws Exception {

		rx.observers.TestSubscriber<Boolean> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.exists(dave.getId()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(true);
	}

	@Test // DATAMONGO-1444
	public void existsWithSingleRxJavaIdMethodsShouldWork() throws Exception {

		rx.observers.TestSubscriber<Boolean> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.exists(Single.just(dave.getId())).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(true);
	}

	@Test // DATAMONGO-1444
	public void singleRxJavaQueryMethodShouldWork() throws Exception {

		rx.observers.TestSubscriber<ReactivePerson> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findByFirstnameAndLastname(dave.getFirstname(), dave.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(dave);
	}

	@Test // DATAMONGO-1444
	public void singleProjectedRxJavaQueryMethodShouldWork() throws Exception {

		rx.observers.TestSubscriber<ProjectedPerson> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findProjectedByLastname(carter.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();

		ProjectedPerson projectedPerson = subscriber.getOnNextEvents().get(0);
		assertThat(projectedPerson.getFirstname(), is(equalTo(carter.getFirstname())));
	}

	@Test // DATAMONGO-1444
	public void observableRxJavaQueryMethodShouldWork() throws Exception {

		rx.observers.TestSubscriber<ReactivePerson> subscriber = new rx.observers.TestSubscriber<>();
		rxJavaPersonRepostitory.findByLastname(boyd.getLastname()).subscribe(subscriber);

		subscriber.awaitTerminalEvent();
		subscriber.assertCompleted();
		subscriber.assertNoErrors();
		subscriber.assertValue(boyd);
	}

	@Test // DATAMONGO-1444
	public void mixedRepositoryShouldWork() throws Exception {

		ReactivePerson value = reactiveRepository.findByLastname(boyd.getLastname()).toBlocking().value();

		assertThat(value, is(equalTo(boyd)));
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneBySingleOfLastName() throws Exception {

		ReactivePerson carter = reactiveRepository.findByLastname(Single.just("Beauford")).block();

		assertThat(carter.getFirstname(), is(equalTo("Carter")));
	}

	@Test // DATAMONGO-1444
	public void shouldFindByObservableOfLastNameIn() throws Exception {

		List<ReactivePerson> persons = reactiveRepository.findByLastnameIn(Observable.just("Beauford", "Matthews"))
				.collectList().block();

		assertThat(persons, hasItems(carter, dave, oliver));
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameInAndAgeGreater() throws Exception {

		List<ReactivePerson> persons = reactiveRepository
				.findByLastnameInAndAgeGreaterThan(Flux.just("Beauford", "Matthews"), 41).toList().toBlocking().single();

		assertThat(persons, hasItems(carter, dave));
	}

	@Repository
	interface ReactivePersonRepostitory extends ReactiveSortingRepository<ReactivePerson, String> {

		Publisher<ReactivePerson> findByLastname(String lastname);
	}

	@Repository
	interface RxJavaPersonRepostitory extends RxJava1SortingRepository<ReactivePerson, String> {

		Observable<ReactivePerson> findByFirstnameAndLastname(String firstname, String lastname);

		Single<ReactivePerson> findByLastname(String lastname);

		Single<ProjectedPerson> findProjectedByLastname(String lastname);
	}

	@Repository
	interface MixedReactivePersonRepostitory extends ReactiveMongoRepository<ReactivePerson, String> {

		Single<ReactivePerson> findByLastname(String lastname);

		Mono<ReactivePerson> findByLastname(Single<String> lastname);

		Flux<ReactivePerson> findByLastnameIn(Observable<String> lastname);

		Flux<ReactivePerson> findByLastname(String lastname, Sort sort);

		Observable<ReactivePerson> findByLastnameInAndAgeGreaterThan(Flux<String> lastname, int age);
	}

	@Document
	@Data
	@NoArgsConstructor
	static class ReactivePerson {

		@Id String id;

		String firstname;
		String lastname;
		int age;

		public ReactivePerson(String firstname, String lastname, int age) {

			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}
	}

	interface ProjectedPerson {

		String getId();

		String getFirstname();
	}
}
