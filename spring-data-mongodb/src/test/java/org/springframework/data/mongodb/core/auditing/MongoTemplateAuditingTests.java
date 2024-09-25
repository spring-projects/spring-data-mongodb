/*
 * Copyright 2019-2024 the original author or authors.
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
package org.springframework.data.mongodb.core.auditing;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.KAuditableVersionedEntity;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
public class MongoTemplateAuditingTests {

	static @Client MongoClient mongoClient;

	@Configuration
	@EnableMongoAuditing
	static class Conf extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return "mongo-template-audit-tests";
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
			return new HashSet<>(Arrays.asList(ImmutableAuditableEntityWithVersion.class, KAuditableVersionedEntity.class));
		}
	}

	@Autowired MongoTemplate template;

	@Test // DATAMONGO-2346
	public void auditingSetsLastModifiedDateCorrectlyForImmutableVersionedEntityOnSave() throws InterruptedException {

		template.remove(new Query(), ImmutableAuditableEntityWithVersion.class);

		ImmutableAuditableEntityWithVersion entity = new ImmutableAuditableEntityWithVersion("id-1", "value", null, null);
		ImmutableAuditableEntityWithVersion inserted = template.save(entity);

		TimeUnit.MILLISECONDS.sleep(500);

		ImmutableAuditableEntityWithVersion modified = inserted.withValue("changed-value");
		ImmutableAuditableEntityWithVersion updated = template.save(modified);

		ImmutableAuditableEntityWithVersion fetched = template.findOne(Query.query(Criteria.where("id").is(entity.id)),
				ImmutableAuditableEntityWithVersion.class);

		assertThat(updated.modificationDate).isAfter(inserted.modificationDate);
		assertThat(fetched.modificationDate).isAfter(inserted.modificationDate);
		assertThat(fetched.modificationDate).isEqualTo(updated.modificationDate.truncatedTo(ChronoUnit.MILLIS));
	}

	@Test // DATAMONGO-2346
	public void auditingSetsLastModifiedDateCorrectlyForImmutableVersionedKotlinEntityOnSave()
			throws InterruptedException {

		template.remove(new Query(), KAuditableVersionedEntity.class);

		KAuditableVersionedEntity entity = new KAuditableVersionedEntity("kId-1", "value", null, null);
		KAuditableVersionedEntity inserted = template.save(entity);

		TimeUnit.MILLISECONDS.sleep(500);

		KAuditableVersionedEntity updated = template.save(inserted.withValue("changed-value"));

		KAuditableVersionedEntity fetched = template.findOne(Query.query(Criteria.where("id").is(entity.getId())),
				KAuditableVersionedEntity.class);

		assertThat(updated.getModificationDate()).isAfter(inserted.getModificationDate());
		assertThat(fetched.getModificationDate()).isAfter(inserted.getModificationDate());
		assertThat(fetched.getModificationDate()).isEqualTo(updated.getModificationDate().truncatedTo(ChronoUnit.MILLIS));
	}

	static class ImmutableAuditableEntityWithVersion {

		final @Id String id;
		final String value;
		final @Version Integer version;
		final @LastModifiedDate Instant modificationDate;

		ImmutableAuditableEntityWithVersion(String id, String value, Integer version, Instant modificationDate) {

			this.id = id;
			this.value = value;
			this.version = version;
			this.modificationDate = modificationDate;
		}

		ImmutableAuditableEntityWithVersion withValue(String value) {
			return new ImmutableAuditableEntityWithVersion(id, value, version, modificationDate);
		}

		ImmutableAuditableEntityWithVersion withModificationDate(Instant modificationDate) {
			return new ImmutableAuditableEntityWithVersion(id, value, version, modificationDate);
		}

		ImmutableAuditableEntityWithVersion withVersion(Integer version) {
			return new ImmutableAuditableEntityWithVersion(id, value, version, modificationDate);
		}
	}
}
