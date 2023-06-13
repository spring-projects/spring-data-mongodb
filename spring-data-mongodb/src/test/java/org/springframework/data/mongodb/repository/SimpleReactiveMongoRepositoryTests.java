/*
 * Copyright 2016-2023 the original author or authors.
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
import static org.springframework.data.domain.ExampleMatcher.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.repository.support.ReactiveMongoRepositoryFactory;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.repository.query.FluentQuery;
import org.springframework.data.repository.query.ReactiveQueryMethodEvaluationContextProvider;
import org.springframework.lang.Nullable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ClassUtils;

/**
 * Tests for {@link ReactiveMongoRepository}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Ruben J Garcia
 * @author Clément Petit
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration("classpath:reactive-infrastructure.xml")
public class SimpleReactiveMongoRepositoryTests implements BeanClassLoaderAware, BeanFactoryAware {

	@Autowired private ReactiveMongoTemplate template;

	private ReactiveMongoRepositoryFactory factory;
	private ClassLoader classLoader;
	private BeanFactory beanFactory;
	private ReactivePersonRepository repository;
	private ReactiveImmutablePersonRepository immutableRepository;

	private ReactivePerson dave, oliver, carter, boyd, stefan, leroi, alicia;
	private ImmutableReactivePerson keith, james, mariah;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader == null ? ClassUtils.getDefaultClassLoader() : classLoader;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@BeforeEach
	void setUp() {

		factory = new ReactiveMongoRepositoryFactory(template);
		factory.setRepositoryBaseClass(SimpleReactiveMongoRepository.class);
		factory.setBeanClassLoader(classLoader);
		factory.setBeanFactory(beanFactory);
		factory.setEvaluationContextProvider(ReactiveQueryMethodEvaluationContextProvider.DEFAULT);

		repository = factory.getRepository(ReactivePersonRepository.class);
		immutableRepository = factory.getRepository(ReactiveImmutablePersonRepository.class);

		repository.deleteAll().as(StepVerifier::create).verifyComplete();
		immutableRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave = new ReactivePerson("Dave", "Matthews", 42);
		oliver = new ReactivePerson("Oliver August", "Matthews", 4);
		carter = new ReactivePerson("Carter", "Beauford", 49);
		boyd = new ReactivePerson("Boyd", "Tinsley", 45);
		stefan = new ReactivePerson("Stefan", "Lessard", 34);
		leroi = new ReactivePerson("Leroi", "Moore", 41);
		alicia = new ReactivePerson("Alicia", "Keys", 30);
		keith = new ImmutableReactivePerson(null, "Keith", "Urban", 53);
		james = new ImmutableReactivePerson(null, "James", "Arthur", 33);
		mariah = new ImmutableReactivePerson(null, "Mariah", "Carey", 51);

		repository.saveAll(Arrays.asList(oliver, dave, carter, boyd, stefan, leroi, alicia)).as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByIdShouldReturnTrueForExistingObject() {
		repository.existsById(dave.id).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByIdShouldReturnFalseForAbsentObject() {
		repository.existsById("unknown").as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByMonoOfIdShouldReturnTrueForExistingObject() {
		repository.existsById(Mono.just(dave.id)).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void existsByFluxOfIdShouldReturnTrueForExistingObject() {
		repository.existsById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void existsByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.existsById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdShouldReturnObject() {
		repository.findById(dave.id).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdShouldCompleteWithoutValueForAbsentObject() {
		repository.findById("unknown").as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdByMonoOfIdShouldReturnTrueForExistingObject() {
		repository.findById(Mono.just(dave.id)).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void findByIdByFluxOfIdShouldReturnTrueForExistingObject() {
		repository.findById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findByIdByEmptyMonoOfIdShouldReturnEmptyMono() {
		repository.findById(Mono.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllShouldReturnAllResults() {
		repository.findAll().as(StepVerifier::create).expectNextCount(7).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByIterableOfIdShouldReturnResults() {
		repository.findAllById(Arrays.asList(dave.id, boyd.id)).as(StepVerifier::create).expectNextCount(2)
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByPublisherOfIdShouldReturnResults() {
		repository.findAllById(Flux.just(dave.id, boyd.id)).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllByEmptyPublisherOfIdShouldReturnResults() {
		repository.findAllById(Flux.empty()).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void findAllWithSortShouldReturnResults() {

		repository.findAll(Sort.by(new Order(Direction.ASC, "age"))).as(StepVerifier::create) //
				.expectNextCount(7) //
				.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void countShouldReturnNumberOfRecords() {
		repository.count().as(StepVerifier::create).expectNext(7L).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void insertEntityShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.insert(person).as(StepVerifier::create).expectNext(person).verifyComplete();

		assertThat(person.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void insertShouldDeferredWrite() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.insert(person);

		assertThat(person.getId()).isNull();
	}

	@Test // DATAMONGO-1444
	void insertIterableOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.insert(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create) //
				.expectNext(dave, oliver, boyd) //
				.verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void insertPublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.insert(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void saveEntityShouldUpdateExistingEntity() {

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.save(dave).as(StepVerifier::create).expectNext(dave).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(dave.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(dave.getLastname());
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void saveEntityShouldInsertNewEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		repository.save(person).as(StepVerifier::create).expectNext(person).verifyComplete();

		repository.findById(person.id).as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.getFirstname()).isEqualTo(person.getFirstname());
			assertThat(actual.getLastname()).isEqualTo(person.getLastname());
		}).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void saveIterableOfNewEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.saveAll(Arrays.asList(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // DATAMONGO-1444
	void saveIterableOfMixedEntitiesShouldInsertEntity() {

		ReactivePerson person = new ReactivePerson("Homer", "Simpson", 36);

		dave.setFirstname("Hello, Dave");
		dave.setLastname("Bowman");

		repository.saveAll(Arrays.asList(person, dave)).as(StepVerifier::create).expectNextCount(2).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).expectNext(dave).verifyComplete();

		assertThat(person.id).isNotNull();
		repository.findById(person.id).as(StepVerifier::create).expectNext(person).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void savePublisherOfEntitiesShouldInsertEntity() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		dave.setId(null);
		oliver.setId(null);
		boyd.setId(null);

		repository.saveAll(Flux.just(dave, oliver, boyd)).as(StepVerifier::create).expectNextCount(3).verifyComplete();

		assertThat(dave.getId()).isNotNull();
		assertThat(oliver.getId()).isNotNull();
		assertThat(boyd.getId()).isNotNull();
	}

	@Test // GH-3609
	void savePublisherOfImmutableEntitiesShouldInsertEntity() {

		immutableRepository.deleteAll().as(StepVerifier::create).verifyComplete();

		immutableRepository.saveAll(Flux.just(keith)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.id).isNotNull();
				}) //
			.verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteAllShouldRemoveEntities() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		repository.findAll().as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteByIdShouldRemoveEntity() {

		repository.deleteById(dave.id).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void deleteByIdUsingMonoShouldRemoveEntity() {

		repository.deleteById(Mono.just(dave.id)).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.id).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1712
	void deleteByIdUsingFluxShouldRemoveEntity() {

		repository.deleteById(Flux.just(dave.id, oliver.id)).as(StepVerifier::create).verifyComplete();

		repository.existsById(dave.id).as(StepVerifier::create).expectNext(false).verifyComplete();
		repository.existsById(oliver.id).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deleteShouldRemoveEntity() {

		repository.delete(dave).as(StepVerifier::create).verifyComplete();

		repository.findById(dave.id).as(StepVerifier::create).verifyComplete();

	}

	@Test // DATAMONGO-1444
	void deleteIterableOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Arrays.asList(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.id).as(StepVerifier::create).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();
	}

	@Test // DATAMONGO-1444
	void deletePublisherOfEntitiesShouldRemoveEntities() {

		repository.deleteAll(Flux.just(dave, boyd)).as(StepVerifier::create).verifyComplete();

		repository.findById(boyd.id).as(StepVerifier::create).verifyComplete();

		repository.findByLastname("Matthews").as(StepVerifier::create).expectNext(oliver).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findOneByExampleShouldReturnObject() {

		Example<ReactivePerson> example = Example.of(dave);

		repository.findOne(example).as(StepVerifier::create).expectNext(dave).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findAllByExampleShouldReturnObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.findAll(example).as(StepVerifier::create).expectNextCount(2).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findAllByExampleAndSortShouldReturnObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.findAll(example, Sort.by("firstname")).as(StepVerifier::create).expectNext(dave, oliver)
				.verifyComplete();
	}

	@Test // DATAMONGO-1619
	void countByExampleShouldCountObjects() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.count(example).as(StepVerifier::create).expectNext(2L).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void existsByExampleShouldReturnExisting() {

		Example<ReactivePerson> example = Example.of(dave, matching().withIgnorePaths("id", "age", "firstname"));

		repository.exists(example).as(StepVerifier::create).expectNext(true).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void existsByExampleShouldReturnNonExisting() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson("foo", "bar", -1));

		repository.exists(example).as(StepVerifier::create).expectNext(false).verifyComplete();
	}

	@Test // DATAMONGO-1619
	void findOneShouldEmitIncorrectResultSizeDataAccessExceptionWhenMoreThanOneElementFound() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson(null, "Matthews", -1),
				matching().withIgnorePaths("age"));

		repository.findOne(example).as(StepVerifier::create).expectError(IncorrectResultSizeDataAccessException.class);
	}

	@Test // DATAMONGO-1907
	void findOneByExampleWithoutResultShouldCompleteEmpty() {

		Example<ReactivePerson> example = Example.of(new ReactivePerson("foo", "bar", -1));

		repository.findOne(example).as(StepVerifier::create).verifyComplete();
	}

	@Test // GH-3757
	void findByShouldReturnFirstResult() {

		ReactivePerson probe = new ReactivePerson();
		probe.setFirstname(oliver.getFirstname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::first) //
				.as(StepVerifier::create) //
				.expectNext(oliver) //
				.verifyComplete();
	}

	@Test // GH-3757
	void findByShouldReturnOneResult() {

		ReactivePerson probe = new ReactivePerson();
		probe.setFirstname(oliver.getFirstname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::one) //
				.as(StepVerifier::create) //
				.expectNext(oliver) //
				.verifyComplete();

		probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::one) //
				.as(StepVerifier::create) //
				.verifyError(IncorrectResultSizeDataAccessException.class);
	}

	@Test // GH-3757
	void findByShouldReturnAll() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::all) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // GH-3757
	void findByShouldApplySortAll() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), it -> it.sortBy(Sort.by("firstname")).all()) //
				.as(StepVerifier::create) //
				.expectNext(dave, oliver) //
				.verifyComplete();

		repository
				.findBy(Example.of(probe, matching().withIgnorePaths("age")),
						it -> it.sortBy(Sort.by(Direction.DESC, "firstname")).all()) //
				.as(StepVerifier::create) //
				.expectNext(oliver, dave) //
				.verifyComplete();
	}

	@Test // GH-3757
	void findByShouldApplyProjection() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), it -> it.project("firstname").first()) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getFirstname()).isNotNull();
					assertThat(it.getLastname()).isNull();
				}).verifyComplete();
	}

	@Test // GH-3757
	void findByShouldApplyPagination() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository
				.findBy(Example.of(probe, matching().withIgnorePaths("age")),
						it -> it.page(PageRequest.of(0, 1, Sort.by("firstname")))) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getTotalElements()).isEqualTo(2);
					assertThat(it.getContent()).contains(dave);
				}).verifyComplete();

		repository
				.findBy(Example.of(probe, matching().withIgnorePaths("age")),
						it -> it.page(PageRequest.of(1, 1, Sort.by("firstname")))) //
				.as(StepVerifier::create) //
				.assertNext(it -> {

					assertThat(it.getTotalElements()).isEqualTo(2);
					assertThat(it.getContent()).contains(oliver);
				}).verifyComplete();
	}

	@Test // GH-3757
	void findByShouldCount() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::count) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		probe = new ReactivePerson();
		probe.setLastname("foo");

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::count) //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();
	}

	@Test // GH-3757
	void findByShouldReportExists() {

		ReactivePerson probe = new ReactivePerson();
		probe.setLastname(oliver.getLastname());

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::exists) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		probe = new ReactivePerson();
		probe.setLastname("foo");

		repository.findBy(Example.of(probe, matching().withIgnorePaths("age")), FluentQuery.ReactiveFluentQuery::exists) //
				.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	interface ReactivePersonRepository extends ReactiveMongoRepository<ReactivePerson, String> {

		Flux<ReactivePerson> findByLastname(String lastname);

	}

	interface ReactiveImmutablePersonRepository extends ReactiveMongoRepository<ImmutableReactivePerson, String> {

	}

	static class ReactivePerson {

		@Id String id;

		String firstname;
		String lastname;
		int age;

		public ReactivePerson() {}

		ReactivePerson(String firstname, String lastname, int age) {

			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public int getAge() {
			return this.age;
		}

		public void setId(String id) {
			this.id = id;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ReactivePerson that = (ReactivePerson) o;
			return age == that.age && Objects.equals(id, that.id) && Objects.equals(firstname, that.firstname)
					&& Objects.equals(lastname, that.lastname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, lastname, age);
		}

		public String toString() {
			return "SimpleReactiveMongoRepositoryTests.ReactivePerson(id=" + this.getId() + ", firstname="
					+ this.getFirstname() + ", lastname=" + this.getLastname() + ", age=" + this.getAge() + ")";
		}
	}

	static final class ImmutableReactivePerson {

		@Id private final String id;

		private final String firstname;
		private final String lastname;
		private final int age;

		ImmutableReactivePerson(@Nullable String id, String firstname, String lastname, int age) {

			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public int getAge() {
			return this.age;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ImmutableReactivePerson that = (ImmutableReactivePerson) o;
			return age == that.age && Objects.equals(id, that.id) && Objects.equals(firstname, that.firstname)
					&& Objects.equals(lastname, that.lastname);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, firstname, lastname, age);
		}

		public String toString() {
			return "SimpleReactiveMongoRepositoryTests.ImmutableReactivePerson(id=" + this.getId() + ", firstname="
					+ this.getFirstname() + ", lastname=" + this.getLastname() + ", age=" + this.getAge() + ")";
		}

		public ImmutableReactivePerson withId(String id) {
			return this.id == id ? this : new ImmutableReactivePerson(id, this.firstname, this.lastname, this.age);
		}

		public ImmutableReactivePerson withFirstname(String firstname) {
			return this.firstname == firstname ? this
					: new ImmutableReactivePerson(this.id, firstname, this.lastname, this.age);
		}

		public ImmutableReactivePerson withLastname(String lastname) {
			return this.lastname == lastname ? this
					: new ImmutableReactivePerson(this.id, this.firstname, lastname, this.age);
		}

		public ImmutableReactivePerson withAge(int age) {
			return this.age == age ? this : new ImmutableReactivePerson(this.id, this.firstname, this.lastname, age);
		}
	}

}
