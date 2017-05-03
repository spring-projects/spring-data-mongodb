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

import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
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

/**
 * Test for {@link ReactiveMongoRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class SimpleReactiveMongoRepositoryTests implements BeanClassLoaderAware, BeanFactoryAware {

	@Autowired private ReactiveMongoTemplate template;

	ReactiveMongoRepositoryFactory factory;
	ClassLoader classLoader;
	BeanFactory beanFactory;
	ReactivePersonRepostitory repository;

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

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);

		StepVerifier.create(repository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia))) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void existsByIdShouldReturnTrueForExistingObject() {
		StepVerifier.create(repository.existsById(dave.id)).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void existsByIdShouldReturnFalseForAbsentObject() {
		StepVerifier.create(repository.existsById("unknown")).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void existsByMonoOfIdShouldReturnTrueForExistingObject() {
		StepVerifier.create(repository.existsById(Mono.just(dave.id))).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void existsByEmptyMonoOfIdShouldReturnEmptyMono() {
		StepVerifier.create(repository.existsById(Mono.empty())).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findOneShouldReturnObject() {
		StepVerifier.create(repository.findById(dave.id)).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findOneShouldCompleteWithoutValueForAbsentObject() {
		StepVerifier.create(repository.findById("unknown")).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findOneByMonoOfIdShouldReturnTrueForExistingObject() {
		StepVerifier.create(repository.findById(Mono.just(dave.id))).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findOneByEmptyMonoOfIdShouldReturnEmptyMono() {
		StepVerifier.create(repository.findById(Mono.empty())).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findAllShouldReturnAllResults() {
		StepVerifier.create(repository.findAll()).expectNextCount(7).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findAllByIterableOfIdShouldReturnResults() {
		StepVerifier.create(repository.findAllById(Arrays.asList(dave.id, boyd.id))).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findAllByPublisherOfIdShouldReturnResults() {
		StepVerifier.create(repository.findAllById(Flux.just(dave.id, boyd.id))).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findAllByEmptyPublisherOfIdShouldReturnResults() {
		StepVerifier.create(repository.findAllById(Flux.empty())).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void findAllWithSortShouldReturnResults() {

		StepVerifier.create(repository.findAll(Sort.by(new Order(Direction.ASC, "age")))) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void countShouldReturnNumberOfRecords() {
		StepVerifier.create(repository.count()).expectNext(7L).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void insertEntityShouldInsertEntity() {

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		StepVerifier.create(repository.insert(person)).expectNext(person).verifyComplete();

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

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		StepVerifier.create(repository.insert(Arrays.asList(dave, oliver, boyd))) //
				.expectNext(dave, oliver, boyd) //
				.verifyComplete();

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void insertPublisherOfEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		StepVerifier.create(repository.insert(Flux.just(dave, oliver, boyd))).expectNextCount(3).verifyComplete();

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		StepVerifier.create(repository.save(dave)).expectNext(dave).verifyComplete();

		StepVerifier.create(repository.findByLastname("Matthews")).expectNext(oliver).verifyComplete();

		StepVerifier.create(repository.findById(dave.id)).consumeNextWith(actual -> {

			assertThat(actual.getFirstname(), is(equalTo(dave.getFirstname())));
			assertThat(actual.getLastname(), is(equalTo(dave.getLastname())));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void saveEntityShouldInsertNewEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		StepVerifier.create(repository.save(person)).expectNext(person).verifyComplete();

		StepVerifier.create(repository.findById(person.id)).consumeNextWith(actual -> {

			assertThat(actual.getFirstname(), is(equalTo(person.getFirstname())));
			assertThat(actual.getLastname(), is(equalTo(person.getLastname())));
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void saveIterableOfNewEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		StepVerifier.create(repository.saveAll(Arrays.asList(dave, oliver, boyd))).expectNextCount(3).verifyComplete();

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void saveIterableOfMixedEntitiesShouldInsertEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		StepVerifier.create(repository.saveAll(Arrays.asList(person, dave))).expectNextCount(2).verifyComplete();

		StepVerifier.create(repository.findById(dave.id)).expectNext(dave).verifyComplete();

		assertThat(person.id, is(notNullValue()));
		StepVerifier.create(repository.findById(person.id)).expectNext(person).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void savePublisherOfEntitiesShouldInsertEntity() {

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		StepVerifier.create(repository.saveAll(Flux.just(dave, oliver, boyd))).expectNextCount(3).verifyComplete();

		assertThat(dave.getId(), is(notNullValue()));
		assertThat(oliver.getId(), is(notNullValue()));
		assertThat(boyd.getId(), is(notNullValue()));
	}

	@Test // DATAMONGO-1444
	public void deleteAllShouldRemoveEntities() {

		StepVerifier.create(repository.deleteAll()).verifyComplete();

		StepVerifier.create(repository.findAll()).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void deleteByIdShouldRemoveEntity() {

		StepVerifier.create(repository.deleteById(dave.id)).verifyComplete();

		StepVerifier.create(repository.findById(dave.id)).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void deleteShouldRemoveEntity() {

		StepVerifier.create(repository.delete(dave)).verifyComplete();

		StepVerifier.create(repository.findById(dave.id)).verifyComplete();

	}

	@Test // DATAMONGO-1444
	public void deleteIterableOfEntitiesShouldRemoveEntities() {

		StepVerifier.create(repository.deleteAll(Arrays.asList(dave, boyd))).verifyComplete();

		StepVerifier.create(repository.findById(boyd.id)).verifyComplete();

		StepVerifier.create(repository.findByLastname("Matthews")).expectNext(oliver).verifyComplete();
	}

	@Test // DATAMONGO-1444
	public void deletePublisherOfEntitiesShouldRemoveEntities() {

		StepVerifier.create(repository.deleteAll(Flux.just(dave, boyd))).verifyComplete();

		StepVerifier.create(repository.findById(boyd.id)).verifyComplete();

		StepVerifier.create(repository.findByLastname("Matthews")).expectNext(oliver).verifyComplete();
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
