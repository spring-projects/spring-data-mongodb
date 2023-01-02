/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.CodecRegistryProvider;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.mongodb.MongoClientSettings;

/**
 * Unit test for {@link com.mongodb.internal.operation.UpdateOperation}.
 *
 * @author Christoph Strobl
 */
class UpdateOperationsUnitTests {

	static final Document SHARD_KEY = new Document("country", "AT").append("userid", "4230");
	static final Document SOURCE_DOC = appendShardKey(new Document("_id", "id-1"));

	MongoMappingContext mappingContext = new MongoMappingContext();
	MongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext);
	QueryMapper queryMapper = new QueryMapper(mongoConverter);
	UpdateMapper updateMapper = new UpdateMapper(mongoConverter);
	EntityOperations entityOperations = new EntityOperations(mongoConverter, this.queryMapper);
	PropertyOperations propertyOperations = new PropertyOperations(mongoConverter.getMappingContext());

	ExtendedQueryOperations queryOperations = new ExtendedQueryOperations(queryMapper, updateMapper, entityOperations, propertyOperations,
			MongoClientSettings::getDefaultCodecRegistry);

	@Test // DATAMONGO-2341
	void appliesShardKeyToFilter() {

		Document sourceFilter = new Document("name", "kaladin");
		assertThat(shardedFilter(sourceFilter, ShardedEntityWithNonDefaultShardKey.class, null))
				.isEqualTo(appendShardKey(sourceFilter));
	}

	@Test
	void applyShardKeyDoesNotAlterSourceFilter() {

		Document sourceFilter = new Document("name", "kaladin");
		shardedFilter(sourceFilter, ShardedEntityWithNonDefaultShardKey.class, null);
		assertThat(sourceFilter).isEqualTo(new Document("name", "kaladin"));
	}

	@Test // DATAMONGO-2341
	void appliesExistingShardKeyToFilter() {

		Document sourceFilter = new Document("name", "kaladin");
		Document existing = new Document("country", "GB").append("userid", "007");

		assertThat(shardedFilter(sourceFilter, ShardedEntityWithNonDefaultShardKey.class, existing))
				.isEqualTo(new Document(existing).append("name", "kaladin"));
	}

	@Test // DATAMONGO-2341
	void recognizesExistingShardKeyInFilter() {

		Document sourceFilter = appendShardKey(new Document("name", "kaladin"));

		assertThat(queryOperations.replaceSingleContextFor(SOURCE_DOC).requiresShardKey(sourceFilter,
				entityOf(ShardedEntityWithNonDefaultShardKey.class))).isFalse();
	}

	@Test // DATAMONGO-2341
	void recognizesIdPropertyAsShardKey() {

		Document sourceFilter = new Document("_id", "id-1");

		assertThat(queryOperations.replaceSingleContextFor(SOURCE_DOC).requiresShardKey(sourceFilter,
				entityOf(ShardedEntityWithDefaultShardKey.class))).isFalse();
	}

	@Test // DATAMONGO-2341
	void returnsMappedShardKey() {

		queryOperations.replaceSingleContextFor(SOURCE_DOC)
				.getMappedShardKeyFields(entityOf(ShardedEntityWithDefaultShardKey.class))
				.containsAll(Arrays.asList("country", "userid"));
	}

	@NonNull
	private Document shardedFilter(Document sourceFilter, Class<?> entity, Document existing) {
		return queryOperations.replaceSingleContextFor(SOURCE_DOC).applyShardKey(entity, sourceFilter, existing);
	}

	private static Document appendShardKey(Document source) {

		Document target = new Document(source);
		target.putAll(SHARD_KEY);
		return target;
	}

	MongoPersistentEntity<?> entityOf(Class<?> type) {
		return mappingContext.getPersistentEntity(type);
	}

	class ExtendedQueryOperations extends QueryOperations {

		ExtendedQueryOperations(QueryMapper queryMapper, UpdateMapper updateMapper, EntityOperations entityOperations, PropertyOperations propertyOperations,
				CodecRegistryProvider codecRegistryProvider) {
			super(queryMapper, updateMapper, entityOperations, propertyOperations, codecRegistryProvider);
		}

		@NonNull
		private ExtendedUpdateContext replaceSingleContextFor(Document source) {
			return new ExtendedUpdateContext(MappedDocument.of(source), true);
		}

		MongoPersistentEntity<?> entityOf(Class<?> type) {
			return mappingContext.getPersistentEntity(type);
		}

		class ExtendedUpdateContext extends UpdateContext {

			ExtendedUpdateContext(MappedDocument update, boolean upsert) {
				super(update, upsert);
			}

			<T> Document applyShardKey(Class<T> domainType, Document filter, @Nullable Document existing) {
				return applyShardKey(entityOf(domainType), filter, existing);
			}
		}
	}
}
