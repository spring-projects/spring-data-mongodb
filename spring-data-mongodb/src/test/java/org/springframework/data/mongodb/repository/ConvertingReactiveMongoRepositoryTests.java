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

import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.observers.TestObserver;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import rx.Observable;
import rx.Single;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.repository.reactive.RxJava2SortingRepository;
import org.springframework.stereotype.Repository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test for {@link ReactiveMongoRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ConvertingReactiveMongoRepositoryTests.Config.class)
public class ConvertingReactiveMongoRepositoryTests {

	@EnableReactiveMongoRepositories(includeFilters = @Filter(value = Repository.class),
			considerNestedRepositories = true)
	@ImportResource("classpath:reactive-infrastructure.xml")
	static class Config {}

	@Autowired MixedReactivePersonRepostitory reactiveRepository;
	@Autowired ReactivePersonRepostitory reactivePersonRepostitory;
	@Autowired RxJava1PersonRepostitory rxJava1PersonRepostitory;
	@Autowired RxJava2PersonRepostitory rxJava2PersonRepostitory;

	ReactivePerson dave, oliver, carter, boyd, stefan, leroi, alicia;

	@Before
	public void setUp() {

		reactiveRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);

		reactiveRepository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia))
				.as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void reactiveStreamsMethodsShouldWork() {
		reactivePersonRepostitory.existsById(dave.getId()).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void reactiveStreamsQueryMethodsShouldWork() {
		StepVerifier.create(reactivePersonRepostitory.findByLastname(boyd.getLastname())).expectNext(boyd).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void simpleRxJava1MethodsShouldWork() {

		rxJava1PersonRepostitory.existsById(dave.getId()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(true) //
				.assertNoErrors() //
				.assertCompleted();
	}

	@Test // DATAMONGO-1444
	public void existsWithSingleRxJava1IdMethodsShouldWork() {

		rxJava1PersonRepostitory.existsById(Single.just(dave.getId())) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(true) //
				.assertNoErrors() //
				.assertCompleted();
	}

	@Test // DATAMONGO-1444
	public void singleRxJava1QueryMethodShouldWork() {

		rxJava1PersonRepostitory.findByFirstnameAndLastname(dave.getFirstname(), dave.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(dave) //
				.assertNoErrors() //
				.assertCompleted();
	}

	@Test // DATAMONGO-1444
	public void singleProjectedRxJava1QueryMethodShouldWork() {

		List<ProjectedPerson> people = rxJava1PersonRepostitory.findProjectedByLastname(carter.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValueCount(1) //
				.assertNoErrors() //
				.assertCompleted() //
				.getOnNextEvents();

		ProjectedPerson projectedPerson = people.get(0);
		assertThat(projectedPerson.getFirstname()).isEqualTo(carter.getFirstname());
	}

	@Test // DATAMONGO-1444
	public void observableRxJava1QueryMethodShouldWork() {

		rxJava1PersonRepostitory.findByLastname(boyd.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(boyd) //
				.assertNoErrors() //
				.assertCompleted() //
				.getOnNextEvents();
	}

	@Test // DATAMONGO-1610
	public void simpleRxJava2MethodsShouldWork() {

		TestObserver<Boolean> testObserver = rxJava2PersonRepostitory.existsById(dave.getId()).test();

		testObserver.awaitTerminalEvent();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(true);
	}

	@Test // DATAMONGO-1610
	public void existsWithSingleRxJava2IdMethodsShouldWork() {

		TestObserver<Boolean> testObserver = rxJava2PersonRepostitory.existsById(io.reactivex.Single.just(dave.getId()))
				.test();

		testObserver.awaitTerminalEvent();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(true);
	}

	@Test // DATAMONGO-1610
	public void flowableRxJava2QueryMethodShouldWork() {

		io.reactivex.subscribers.TestSubscriber<ReactivePerson> testSubscriber = rxJava2PersonRepostitory
				.findByFirstnameAndLastname(dave.getFirstname(), dave.getLastname()).test();

		testSubscriber.awaitTerminalEvent();
		testSubscriber.assertComplete();
		testSubscriber.assertNoErrors();
		testSubscriber.assertValue(dave);
	}

	@Test // DATAMONGO-1610
	public void singleProjectedRxJava2QueryMethodShouldWork() {

		TestObserver<ProjectedPerson> testObserver = rxJava2PersonRepostitory
				.findProjectedByLastname(Maybe.just(carter.getLastname())).test();

		testObserver.awaitTerminalEvent();
		testObserver.assertComplete();
		testObserver.assertNoErrors();

		testObserver.assertValue(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
			return true;
		});
	}

	@Test // DATAMONGO-1610
	public void observableProjectedRxJava2QueryMethodShouldWork() {

		TestObserver<ProjectedPerson> testObserver = rxJava2PersonRepostitory
				.findProjectedByLastname(Single.just(carter.getLastname())).test();

		testObserver.awaitTerminalEvent();
		testObserver.assertComplete();
		testObserver.assertNoErrors();

		testObserver.assertValue(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
			return true;
		});
	}

	@Test // DATAMONGO-1610
	public void maybeRxJava2QueryMethodShouldWork() {

		TestObserver<ReactivePerson> testObserver = rxJava2PersonRepostitory.findByLastname(boyd.getLastname()).test();

		testObserver.awaitTerminalEvent();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(boyd);
	}

	@Test // DATAMONGO-1444
	public void mixedRepositoryShouldWork() {

		reactiveRepository.findByLastname(boyd.getLastname()) //
				.test() //
				.awaitTerminalEvent() //
				.assertValue(boyd) //
				.assertNoErrors() //
				.assertCompleted() //
				.getOnNextEvents();
	}

	@Test // DATAMONGO-1444
	public void shouldFindOneBySingleOfLastName() {

		reactiveRepository.findByLastname(Single.just(carter.getLastname())).as(StepVerifier::create) //
				.expectNext(carter) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByObservableOfLastNameIn() {

		reactiveRepository.findByLastnameIn(Observable.just(carter.getLastname(), dave.getLastname()))
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void shouldFindByPublisherOfLastNameInAndAgeGreater() {

		List<ReactivePerson> people = reactiveRepository
				.findByLastnameInAndAgeGreaterThan(Flux.just(carter.getLastname(), dave.getLastname()), 41).test() //
				.awaitTerminalEvent() //
				.assertValueCount(2) //
				.assertNoErrors() //
				.assertCompleted() //
				.getOnNextEvents();

		assertThat(people).contains(carter, dave);
	}

	@Repository
	interface ReactivePersonRepostitory extends ReactiveSortingRepository<ReactivePerson, String> {

		Publisher<ReactivePerson> findByLastname(String lastname);
	}

	@Repository
	interface RxJava1PersonRepostitory extends org.springframework.data.repository.Repository<ReactivePerson, String> {

		Observable<ReactivePerson> findByFirstnameAndLastname(String firstname, String lastname);

		Single<ReactivePerson> findByLastname(String lastname);

		Single<ProjectedPerson> findProjectedByLastname(String lastname);

		Single<Boolean> existsById(String id);

		Single<Boolean> existsById(Single<String> id);
	}

	@Repository
	interface RxJava2PersonRepostitory extends RxJava2SortingRepository<ReactivePerson, String> {

		Flowable<ReactivePerson> findByFirstnameAndLastname(String firstname, String lastname);

		Maybe<ReactivePerson> findByLastname(String lastname);

		io.reactivex.Single<ProjectedPerson> findProjectedByLastname(Maybe<String> lastname);

		io.reactivex.Observable<ProjectedPerson> findProjectedByLastname(Single<String> lastname);
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
