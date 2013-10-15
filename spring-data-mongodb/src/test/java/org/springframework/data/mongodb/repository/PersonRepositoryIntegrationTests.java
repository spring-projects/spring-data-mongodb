/*
 * Copyright 2010-2012 the original author or authors.
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

import java.util.List;

import org.junit.Test;
import org.springframework.data.mongodb.domain.PredicateBuilder;
import org.springframework.data.mongodb.domain.QueryDslMongodbPredicate;
import org.springframework.data.mongodb.domain.Specification;
import org.springframework.data.mongodb.domain.Specifications;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration test for {@link PersonRepository}.
 * 
 * @author Oliver Gierke
 */
@ContextConfiguration
public class PersonRepositoryIntegrationTests extends
		AbstractPersonRepositoryIntegrationTests {

	@Test
	public void testSpecification() {

		User user = new User();
		user.username = "Patryk";
		operations.save(user);

		Boss jozef = new Boss();
		jozef.name = "Jozef";
		operations.save(jozef);

		Boss maks = new Boss();
		maks.name = "Maks";
		operations.save(maks);

		Person wojtek = new Person("Wojtek", "Krzaklewski", 29);
		wojtek.creator = user;
		wojtek.boss = jozef;
		repository.save(wojtek);
		
		Person jacek = new Person("Jacek", "Mroz", 29);
		jacek.creator = user;
		jacek.boss = jozef;
		repository.save(jacek);

		Person michal = new Person("Michal", "Ktostam", 31);
		michal.creator = user;
		michal.boss = maks;
		repository.save(michal);

		Specification<Person> specCreatorPatryk = new Specification<Person>() {

			public QueryDslMongodbPredicate<Person> buildPredicate(
					PredicateBuilder<Person> predicateBuilder) {
				return predicateBuilder.where().join(QPerson.person.creator, QUser.user)
						.on(QUser.user.username.eq("Patryk"));
			}
		};
		List<Person> findAll = repository.findAll(specCreatorPatryk);

		assertThat(findAll, hasSize(3));
		assertThat(findAll, hasItem(wojtek));
		assertThat(findAll, hasItem(michal));
		assertThat(findAll, hasItem(jacek));
		
		Specification<Person> specPersonWojtek = new Specification<Person>() {

			public QueryDslMongodbPredicate<Person> buildPredicate(
					PredicateBuilder<Person> predicateBuilder) {
				return predicateBuilder.where(QPerson.person.firstname.eq("Wojtek"));
			}
		};
		
		findAll = repository.findAll(specPersonWojtek);

		assertThat(findAll, hasSize(1));
		assertThat(findAll, hasItem(wojtek));
		
		findAll = repository.findAll(Specifications.where(specCreatorPatryk).and(specPersonWojtek));
		
		assertThat(findAll, hasSize(1));
		assertThat(findAll, hasItem(wojtek));
		

		Specification<Person> specBossJozef = new Specification<Person>() {

			public QueryDslMongodbPredicate<Person> buildPredicate(
					PredicateBuilder<Person> predicateBuilder) {
				return predicateBuilder.where().join(QPerson.person.boss, QBoss.boss)
						.on(QBoss.boss.name.eq("Jozef"));
			}
		};
		findAll = repository.findAll(specBossJozef);
		
		assertThat(findAll, hasSize(2));
		assertThat(findAll, hasItem(wojtek));
		assertThat(findAll, hasItem(jacek));
		
		findAll = repository.findAll(Specifications.where(specCreatorPatryk).and(new Specification<Person>() {

			public QueryDslMongodbPredicate<Person> buildPredicate(
					PredicateBuilder<Person> predicateBuilder) {
				return predicateBuilder.where().join(QPerson.person.boss, QBoss.boss).on(QBoss.boss.name.eq("Maks"));
			}
		}));
		
		assertThat(findAll, hasSize(1));
		assertThat(findAll, hasItem(michal));

	}
}
