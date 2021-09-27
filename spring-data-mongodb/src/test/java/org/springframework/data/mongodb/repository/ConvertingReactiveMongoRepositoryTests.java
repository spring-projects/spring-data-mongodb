/*
 * Copyright 2016-2021 the original author or authors.
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

import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.repository.reactive.RxJava3SortingRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Test for {@link ReactiveMongoRepository} using reactive wrapper type conversion.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = ConvertingReactiveMongoRepositoryTests.Config.class)
public class ConvertingReactiveMongoRepositoryTests {

	@EnableReactiveMongoRepositories(
			includeFilters = { @Filter(value = ReactivePersonRepostitory.class, type = FilterType.ASSIGNABLE_TYPE),
					@Filter(value = RxJava3PersonRepostitory.class, type = FilterType.ASSIGNABLE_TYPE),
					@Filter(value = MixedReactivePersonRepostitory.class, type = FilterType.ASSIGNABLE_TYPE) },
			considerNestedRepositories = true)
	@ImportResource("classpath:reactive-infrastructure.xml")
	static class Config {}

	@Autowired MixedReactivePersonRepostitory reactiveRepository;
	@Autowired ReactivePersonRepostitory reactivePersonRepostitory;
	@Autowired RxJava3PersonRepostitory rxJava3PersonRepostitory;

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

	@Test // DATAMONGO-2558
	public void simpleRxJava3MethodsShouldWork() throws InterruptedException {

		TestObserver<Boolean> testObserver = rxJava3PersonRepostitory.existsById(dave.getId()).test();

		testObserver.await();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(true);
	}

	@Test // DATAMONGO-2558
	public void existsWithSingleRxJava3IdMethodsShouldWork() throws InterruptedException {

		TestObserver<Boolean> testObserver = rxJava3PersonRepostitory.existsById(Single.just(dave.getId()))
				.test();

		testObserver.await();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(true);
	}

	@Test // DATAMONGO-2558
	public void flowableRxJava3QueryMethodShouldWork() throws InterruptedException {

		TestSubscriber<ReactivePerson> testSubscriber = rxJava3PersonRepostitory
				.findByFirstnameAndLastname(dave.getFirstname(), dave.getLastname()).test();

		testSubscriber.await();
		testSubscriber.assertComplete();
		testSubscriber.assertNoErrors();
		testSubscriber.assertValue(dave);
	}

	@Test // DATAMONGO-2558
	public void singleProjectedRxJava3QueryMethodShouldWork() throws InterruptedException {

		io.reactivex.rxjava3.observers.TestObserver<ProjectedPerson> testObserver = rxJava3PersonRepostitory
				.findProjectedByLastname(io.reactivex.rxjava3.core.Maybe.just(carter.getLastname())).test();

		testObserver.await();
		testObserver.assertComplete();
		testObserver.assertNoErrors();

		testObserver.assertValue(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
			return true;
		});
	}

	@Test // DATAMONGO-2558
	public void observableProjectedRxJava3QueryMethodShouldWork() throws InterruptedException {

		io.reactivex.rxjava3.observers.TestObserver<ProjectedPerson> testObserver = rxJava3PersonRepostitory
				.findProjectedByLastname(io.reactivex.rxjava3.core.Single.just(carter.getLastname())).test();

		testObserver.await();
		testObserver.assertComplete();
		testObserver.assertNoErrors();

		testObserver.assertValue(actual -> {
			assertThat(actual.getFirstname()).isEqualTo(carter.getFirstname());
			return true;
		});
	}

	@Test // DATAMONGO-2558
	public void maybeRxJava3QueryMethodShouldWork() throws InterruptedException {

		io.reactivex.rxjava3.observers.TestObserver<ReactivePerson> testObserver = rxJava3PersonRepostitory
				.findByLastname(boyd.getLastname()).test();

		testObserver.await();
		testObserver.assertComplete();
		testObserver.assertNoErrors();
		testObserver.assertValue(boyd);
	}

//	@Test // DATAMONGO-1444
//	public void mixedRepositoryShouldWork() {
//
//		reactiveRepository.findByLastname(boyd.getLastname()) //
//				.test() //
//				.awaitTerminalEvent() //
//				.assertValue(boyd) //
//				.assertNoErrors() //
//				.assertCompleted() //
//				.getOnNextEvents();
//	}

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

//	@Test // DATAMONGO-1444
//	public void shouldFindByPublisherOfLastNameInAndAgeGreater() {
//
//		List<ReactivePerson> people = reactiveRepository
//				.findByLastnameInAndAgeGreaterThan(Flux.just(carter.getLastname(), dave.getLastname()), 41).test() //
//				.awaitTerminalEvent() //
//				.assertValueCount(2) //
//				.assertNoErrors() //
//				.assertCompleted() //
//				.getOnNextEvents();
//
//		assertThat(people).contains(carter, dave);
//	}

	interface ReactivePersonRepostitory extends ReactiveSortingRepository<ReactivePerson, String> {

		Publisher<ReactivePerson> findByLastname(String lastname);
	}

	interface RxJava3PersonRepostitory extends RxJava3SortingRepository<ReactivePerson, String> {

		io.reactivex.rxjava3.core.Flowable<ReactivePerson> findByFirstnameAndLastname(String firstname, String lastname);

		io.reactivex.rxjava3.core.Maybe<ReactivePerson> findByLastname(String lastname);

		io.reactivex.rxjava3.core.Single<ProjectedPerson> findProjectedByLastname(
				io.reactivex.rxjava3.core.Maybe<String> lastname);

		io.reactivex.rxjava3.core.Observable<ProjectedPerson> findProjectedByLastname(
				io.reactivex.rxjava3.core.Single<String> lastname);
	}

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
