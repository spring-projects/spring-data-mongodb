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
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.repository.query.DefaultEvaluationContextProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;

import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.TestSubscriber;

/**
 * Test for {@link ReactiveMongoRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class SimpleReactiveMongoRepositoryTests implements BeanClassLoaderAware, BeanFactoryAware {

	@Autowired private ReactiveMongoTemplate template;

	private ReactiveMongoRepositoryFactory factory;
	private ClassLoader classLoader;
	private BeanFactory beanFactory;
	private ReactivePersonRepostitory repository;

	private ReactivePerson dave, oliver, carter, boyd, stefan, leroi, alicia;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Before
	public void setUp() {

		factory = new ReactiveMongoRepositoryFactory(template);
		factory.setRepositoryBaseClass(SimpleReactiveMongoRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(DefaultEvaluationContextProvider.INSTANCE);

		repository = factory.getRepository(ReactivePersonRepostitory.class);

		repository.deleteAll().block();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);

		TestSubscriber<ReactivePerson> subscriber = TestSubscriber.create();
		repository.save(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia)).subscribe(subscriber);

		subscriber.await().assertComplete().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void existsByIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.exists(dave.id).block();

		assertThat(exists, is(true));
	}

	@Test // DATAMONGO-1444
	public void existsByIdShouldReturnFalseForAbsentObject() {

		TestSubscriber<Boolean> testSubscriber = TestSubscriber.subscribe(repository.exists("unknown"));

		testSubscriber.await().assertComplete().assertValues(false).assertNoError();
	}

	@Test // DATAMONGO-1444
	public void existsByMonoOfIdShouldReturnTrueForExistingObject() {

		Boolean exists = repository.exists(Mono.just(dave.id)).block();
		assertThat(exists, is(true));
	}

	@Test // DATAMONGO-1444
	public void existsByEmptyMonoOfIdShouldReturnEmptyMono() {

		TestSubscriber<Boolean> testSubscriber = TestSubscriber.subscribe(repository.exists(Mono.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void findOneShouldReturnObject() {

		ReactivePerson person = repository.findOne(dave.id).block();

		assertThat(person.getFirstname(), is(equalTo("Dave")));
	}

	@Test // DATAMONGO-1444
	public void findOneShouldCompleteWithoutValueForAbsentObject() {

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.findOne("unknown"));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void findOneByMonoOfIdShouldReturnTrueForExistingObject() {

		ReactivePerson person = repository.findOne(Mono.just(dave.id)).block();

		assertThat(person.id, is(equalTo(dave.id)));
	}

	@Test // DATAMONGO-1444
	public void findOneByEmptyMonoOfIdShouldReturnEmptyMono() {

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.findOne(Mono.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void findAllShouldReturnAllResults() {

		List<ReactivePerson> persons = repository.findAll().collectList().block();

		assertThat(persons, hasSize(7));
	}

	@Test // DATAMONGO-1444
	public void findAllByIterableOfIdShouldReturnResults() {

		List<ReactivePerson> persons = repository.findAll(Arrays.asList(dave.id, boyd.id)).collectList().block();

		assertThat(persons, hasSize(2));
	}

	@Test // DATAMONGO-1444
	public void findAllByPublisherOfIdShouldReturnResults() {

		List<ReactivePerson> persons = repository.findAll(Flux.just(dave.id, boyd.id)).collectList().block();

		assertThat(persons, hasSize(2));
	}

	@Test // DATAMONGO-1444
	public void findAllByEmptyPublisherOfIdShouldReturnResults() {

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.findAll(Flux.empty()));

		testSubscriber.await().assertComplete().assertNoValues().assertNoError();
	}

	@Test // DATAMONGO-1444
	public void findAllWithSortShouldReturnResults() {

		List<ReactivePerson> persons = repository.findAll(new Sort(new Order(Direction.ASC, "age"))).collectList().block();

		assertThat(persons, hasSize(7));
		assertThat(persons.get(0).getId(), is(equalTo(oliver.getId())));
	}

	@Test // DATAMONGO-1444
	public void countShouldReturnNumberOfRecords() {

		TestSubscriber<Long> testSubscriber = TestSubscriber.subscribe(repository.count());

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(7L).assertNoError();
	}

	@Test // DATAMONGO-1444
	public void insertEntityShouldInsertEntity() {

		repository.deleteAll().block();

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.insert(person));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(person);

		assertThat(person.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertShouldDeferredWrite() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.insert(person);

		assertThat(person.getId(), is(nullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber
				.subscribe(repository.insert(Arrays.asList(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3).assertValues(dave, oliver, boyd);

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertPublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber
				.subscribe(repository.insert(Flux.just(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.save(dave));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(dave);

		List<ReactivePerson> matthews = repository.findByLastname("Matthews").collectList().block();
		assertThat(matthews, hasSize(1));
		assertThat(matthews, contains(oliver));
		assertThat(matthews, not(contains(dave)));

		ReactivePerson reactivePerson = repository.findOne(dave.id).block();

		assertThat(reactivePerson.getFirstname(), is(equalTo(dave.getFirstname())));
		assertThat(reactivePerson.getLastname(), is(equalTo(dave.getLastname())));
	}

	@Test // DATAMONGO-1444
	public void saveEntityShouldInsertNewEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.save(person));

		testSubscriber.await().assertComplete().assertValueCount(1).assertValues(person);

		ReactivePerson reactivePerson = repository.findOne(person.id).block();

		assertThat(reactivePerson.getFirstname(), is(equalTo(person.getFirstname())));
		assertThat(reactivePerson.getLastname(), is(equalTo(person.getLastname())));
	}

	@Test // DATAMONGO-1444
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber
				.subscribe(repository.save(Arrays.asList(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3).assertValues(dave, oliver, boyd);

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber
				.subscribe(repository.save(Arrays.asList(person, dave)));

		testSubscriber.await().assertComplete().assertValueCount(2);

		ReactivePerson persistentDave = repository.findOne(dave.id).block();
		assertThat(persistentDave, is(equalTo(dave)));

		assertThat(person.id, is(notNullValue()));
		ReactivePerson persistentHomer = repository.findOne(person.id).block();
		assertThat(persistentHomer, is(equalTo(person)));
	}

	@Test // DATAMONGO-1444
	public void savePublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().block();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber
				.subscribe(repository.save(Flux.just(dave, oliver, boyd)));

		testSubscriber.await().assertComplete().assertValueCount(3);

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void deleteAllShouldRemoveEntities() {

		repository.deleteAll().block();

		TestSubscriber<ReactivePerson> testSubscriber = TestSubscriber.subscribe(repository.findAll());

		testSubscriber.await().assertComplete().assertValueCount(0);
	}

	@Test // DATAMONGO-1444
	public void deleteByIdShouldRemoveEntity() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(dave.id));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<ReactivePerson> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(dave.id));

		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	@Test // DATAMONGO-1444
	public void deleteShouldRemoveEntity() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(dave));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<ReactivePerson> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(dave.id));

		verificationSubscriber.await().assertComplete().assertNoValues();
	}

	@Test // DATAMONGO-1444
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(Arrays.asList(dave, boyd)));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<ReactivePerson> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(boyd.id));
		verificationSubscriber.await().assertComplete().assertNoValues();

		List<ReactivePerson> matthews = repository.findByLastname("Matthews").collectList().block();
		assertThat(matthews, hasSize(1));
		assertThat(matthews, contains(oliver));

	}

	@Test // DATAMONGO-1444
	public void deletePublisherOfEntitiesShouldRemoveEntities() {

		TestSubscriber<Void> testSubscriber = TestSubscriber.subscribe(repository.delete(Flux.just(dave, boyd)));

		testSubscriber.await().assertComplete().assertNoValues();

		TestSubscriber<ReactivePerson> verificationSubscriber = TestSubscriber.subscribe(repository.findOne(boyd.id));
		verificationSubscriber.await().assertComplete().assertNoValues();

		List<ReactivePerson> matthews = repository.findByLastname("Matthews").collectList().block();
		assertThat(matthews, hasSize(1));
		assertThat(matthews, contains(oliver));

	}

	interface ReactivePersonRepostitory extends ReactiveMongoRepository<ReactivePerson, String> {

		Flux<ReactivePerson> findByLastname(String lastname);

	}

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
}
