/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.VersionedPerson;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class SimpleMongoRepositoryVersionedEntityTests {

	@Configuration
	static class Config extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}
	}

	private static final MongoPersistentEntity ENTITY = new BasicMongoPersistentEntity(
			ClassTypeInformation.from(VersionedPerson.class));

	@Autowired //
	private MongoTemplate template;

	private MongoEntityInformation<VersionedPerson, String> personEntityInformation;
	private SimpleMongoRepository<VersionedPerson, String> repository;

	private VersionedPerson sarah;

	@Before
	public void setUp() {

		personEntityInformation = new MappingMongoEntityInformation(ENTITY);
		repository = new SimpleMongoRepository<>(personEntityInformation, template);
		repository.deleteAll();

		sarah = repository.save(new VersionedPerson("Sarah", "Connor"));
	}

	@Test // DATAMONGO-2195
	public void deleteWithMatchingVersion() {

		repository.delete(sarah);

		assertThat(template.count(query(where("id").is(sarah.getId())), VersionedPerson.class)).isZero();
	}

	@Test // DATAMONGO-2195
	public void deleteWithMatchingVersionInTx() {

		long countBefore = repository.count();

		initTxTemplate().execute(status -> {

			VersionedPerson t800 = repository.save(new VersionedPerson("T-800"));
			repository.delete(t800);

			return Void.TYPE;
		});

		assertThat(repository.count()).isEqualTo(countBefore);
	}

	@Test // DATAMONGO-2195
	public void deleteWithVersionMismatch() {

		sarah.setVersion(5L);

		assertThatExceptionOfType(OptimisticLockingFailureException.class).isThrownBy(() -> repository.delete(sarah)) //
				.withMessageContaining("Expected version 5 but was 0");

		assertThat(template.count(query(where("id").is(sarah.getId())), VersionedPerson.class)).isOne();
	}

	@Test // DATAMONGO-2195
	public void deleteWithVersionMismatchInTx() {

		long countBefore = repository.count();

		assertThatExceptionOfType(OptimisticLockingFailureException.class)
				.isThrownBy(() -> initTxTemplate().execute(status -> {

					VersionedPerson t800 = repository.save(new VersionedPerson("T-800"));
					t800.setVersion(5L);
					repository.delete(t800);

					return Void.TYPE;
				}));

		assertThat(repository.count()).isEqualTo(countBefore);
	}

	@Test // DATAMONGO-2195
	public void deleteNonExisting() {
		repository.delete(new VersionedPerson("T-800"));
	}

	@Test // DATAMONGO-2195
	public void deleteNonExistingInTx() {

		initTxTemplate().execute(status -> {

			repository.delete(new VersionedPerson("T-800"));

			return Void.TYPE;
		});
	}

	TransactionTemplate initTxTemplate() {

		MongoTransactionManager txmgr = new MongoTransactionManager(template.getMongoDbFactory());
		TransactionTemplate tt = new TransactionTemplate(txmgr);
		tt.afterPropertiesSet();

		return tt;
	}
}
