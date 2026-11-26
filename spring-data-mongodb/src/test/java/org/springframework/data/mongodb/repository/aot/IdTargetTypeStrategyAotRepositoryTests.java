/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.IdTargetTypeStrategy;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.test.util.MongoTestUtils;
import org.springframework.data.repository.Repository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for AOT repositories generated with the default {@link IdTargetTypeStrategy} and executed using
 * {@link IdTargetTypeStrategy#IMPLICIT}.
 *
 * @author Christoph Strobl
 */
@SpringJUnitConfig(classes = IdTargetTypeStrategyAotRepositoryTests.Config.class)
class IdTargetTypeStrategyAotRepositoryTests {

	static final String DB_NAME = "aot-id-target-type-strategy-tests";
	static final MongoClient client = MongoTestUtils.client();

	@Autowired PlainIdRepository repository;
	@Autowired MongoOperations operations;

	@Configuration
	static class Config extends AotFragmentTestConfigurationSupport {

		Config() {
			super(PlainIdRepository.class);
		}

		@Bean
		MongoOperations mongoOperations() {

			MongoCustomConversions conversions = MongoCustomConversions
					.create(adapter -> adapter.idTargetTypeStrategy(IdTargetTypeStrategy.IMPLICIT));

			MongoMappingContext context = new MongoMappingContext();
			context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
			context.setIdTargetTypeStrategy(conversions.getIdTargetTypeStrategy());
			context.afterPropertiesSet();

			MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
			converter.setCustomConversions(conversions);
			converter.afterPropertiesSet();

			return new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, DB_NAME), converter);
		}
	}

	@AfterEach
	void afterEach() {
		operations.dropCollection(PlainId.class);
	}

	@Test // GH-4466
	void derivedQueryRetainsHexStringId() {

		String id = new ObjectId().toHexString();
		operations.getCollection(operations.getCollectionName(PlainId.class)).insertOne(new Document("_id", id));

		assertThat(repository.findOneById(id)).isEqualTo(new PlainId(id));
		assertThat(repository.findByIdIn(List.of(id))).containsExactly(new PlainId(id));
	}

	@Test // GH-4466
	void stringQueryRetainsHexStringId() {

		String id = new ObjectId().toHexString();
		operations.getCollection(operations.getCollectionName(PlainId.class)).insertOne(new Document("_id", id));

		assertThat(repository.findByStringQuery(id)).isEqualTo(new PlainId(id));
	}

	public record PlainId(@Id String id) {
	}

	public interface PlainIdRepository extends Repository<PlainId, String> {

		PlainId findOneById(String id);

		List<PlainId> findByIdIn(List<String> ids);

		@Query("{ '_id' : ?0 }")
		PlainId findByStringQuery(String id);
	}
}
