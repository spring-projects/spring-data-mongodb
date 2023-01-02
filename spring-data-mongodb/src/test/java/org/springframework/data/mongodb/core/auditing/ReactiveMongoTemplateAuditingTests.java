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
package org.springframework.data.mongodb.core.auditing;

import static org.assertj.core.api.Assertions.*;

import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;
import org.springframework.data.mongodb.config.EnableReactiveMongoAuditing;
import org.springframework.data.mongodb.core.KAuditableVersionedEntity;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.test.util.Client;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.mongodb.reactivestreams.client.MongoClient;

/**
 * Integration tests for {@link EnableReactiveMongoAuditing} through {@link ReactiveMongoTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith({ MongoClientExtension.class, SpringExtension.class })
class ReactiveMongoTemplateAuditingTests {

	static final String DB_NAME = "mongo-template-audit-tests";

	static @Client MongoClient mongoClient;

	@Configuration
	@EnableReactiveMongoAuditing
	static class Conf extends AbstractReactiveMongoConfiguration {

		@Bean
		@Override
		public MongoClient reactiveMongoClient() {
			return mongoClient;
		}

		@Override
		protected String getDatabaseName() {
			return DB_NAME;
		}

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.emptySet();
		}
	}

	@Autowired ReactiveMongoTemplate template;
	@Autowired MongoClient client;

	@BeforeEach
	void setUp() {

		MongoTestUtils.flushCollection(DB_NAME, template.getCollectionName(ImmutableAuditableEntityWithVersion.class),
				client);
		MongoTestUtils.flushCollection(DB_NAME, template.getCollectionName(KAuditableVersionedEntity.class), client);
	}

	@Test // DATAMONGO-2346
	void auditingSetsLastModifiedDateCorrectlyForImmutableVersionedEntityOnSave() {

		ImmutableAuditableEntityWithVersion entity = new ImmutableAuditableEntityWithVersion(null, "value", null, null);

		template.save(entity).delayElement(Duration.ofMillis(500)) //
				.flatMap(inserted -> template.save(inserted.withValue("changed-value")) //
						.map(updated -> Tuples.of(inserted, updated))) //
				.flatMap(tuple2 -> template
						.findOne(Query.query(Criteria.where("id").is(tuple2.getT1().id)), ImmutableAuditableEntityWithVersion.class)
						.map(fetched -> Tuples.of(tuple2.getT1(), tuple2.getT2(), fetched))) //
				.as(StepVerifier::create) //
				.consumeNextWith(tuple3 -> {

					assertThat(tuple3.getT2().modificationDate).isAfter(tuple3.getT1().modificationDate);
					assertThat(tuple3.getT3().modificationDate).isAfter(tuple3.getT1().modificationDate);
					assertThat(tuple3.getT3().modificationDate)
							.isEqualTo(tuple3.getT2().modificationDate.truncatedTo(ChronoUnit.MILLIS));
				}) //
				.verifyComplete();
	}

	@Test // DATAMONGO-2346
	void auditingSetsLastModifiedDateCorrectlyForImmutableVersionedKotlinEntityOnSave() {

		KAuditableVersionedEntity entity = new KAuditableVersionedEntity(null, "value", null, null);

		template.save(entity).delayElement(Duration.ofMillis(500)) //
				.flatMap(inserted -> template.save(inserted.withValue("changed-value")) //
						.map(updated -> Tuples.of(inserted, updated))) //
				.flatMap(tuple2 -> template
						.findOne(Query.query(Criteria.where("id").is(tuple2.getT1().getId())), KAuditableVersionedEntity.class)
						.map(fetched -> Tuples.of(tuple2.getT1(), tuple2.getT2(), fetched))) //
				.as(StepVerifier::create) //
				.consumeNextWith(tuple3 -> {

					assertThat(tuple3.getT2().getModificationDate()).isAfter(tuple3.getT1().getModificationDate());
					assertThat(tuple3.getT3().getModificationDate()).isAfter(tuple3.getT1().getModificationDate());
					assertThat(tuple3.getT3().getModificationDate())
							.isEqualTo(tuple3.getT2().getModificationDate().truncatedTo(ChronoUnit.MILLIS));
				}) //
				.verifyComplete();
	}

	@Document("versioned-auditable")
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

		ImmutableAuditableEntityWithVersion withId(String id) {
			return new ImmutableAuditableEntityWithVersion(id, value, version, modificationDate);
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
