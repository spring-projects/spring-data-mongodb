/*
 * Copyright 2019-2023 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;
import static org.springframework.data.mongodb.core.query.Query.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.repository.VersionedPerson;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.test.util.EnableIfMongoServerVersion;
import org.springframework.data.mongodb.test.util.MongoClientClosingTestConfiguration;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.mongodb.test.util.ReplicaSet;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.support.TransactionTemplate;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class SimpleMongoRepositoryVersionedEntityTests {

	@Configuration
	static class Config extends MongoClientClosingTestConfiguration {

		@Override
		public MongoClient mongoClient() {
			return MongoTestUtils.client();
		}

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return new HashSet<>(Arrays.asList(VersionedPerson.class));
		}
	}

	@Autowired private MongoTemplate template;

	private MongoEntityInformation<VersionedPerson, String> personEntityInformation;
	private SimpleMongoRepository<VersionedPerson, String> repository;

	private VersionedPerson sarah;

	@BeforeEach
	public void setUp() {

		MongoPersistentEntity entity = template.getConverter().getMappingContext()
				.getRequiredPersistentEntity(VersionedPerson.class);

		personEntityInformation = new MappingMongoEntityInformation(entity);
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
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void deleteWithMatchingVersionInTx() {

		assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

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

		assertThatExceptionOfType(OptimisticLockingFailureException.class).isThrownBy(() -> repository.delete(sarah));

		assertThat(template.count(query(where("id").is(sarah.getId())), VersionedPerson.class)).isOne();
	}

	@Test // DATAMONGO-2195
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void deleteWithVersionMismatchInTx() {

		assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

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
		assertThatThrownBy(() -> repository.delete(new VersionedPerson("T-800")))
				.isInstanceOf(OptimisticLockingFailureException.class);
	}

	@Test // DATAMONGO-2195
	@EnableIfMongoServerVersion(isGreaterThanEqual = "4.0")
	public void deleteNonExistingInTx() {

		assumeThat(ReplicaSet.required().runsAsReplicaSet()).isTrue();

		initTxTemplate().execute(status -> {

			assertThatThrownBy(() -> repository.delete(new VersionedPerson("T-800")))
					.isInstanceOf(OptimisticLockingFailureException.class);

			return Void.TYPE;
		});
	}

	TransactionTemplate initTxTemplate() {

		MongoTransactionManager txmgr = new MongoTransactionManager(template.getMongoDatabaseFactory());
		TransactionTemplate tt = new TransactionTemplate(txmgr);
		tt.afterPropertiesSet();

		return tt;
	}
}
