/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import static org.assertj.core.api.Assertions.*;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

/**
 * Unit tests for {@link AggregationMapper}.
 */
public class AggregationMapperUnitTests {

	private AggregationMapper mapper;
	private MongoMappingContext context;
	private MappingMongoConverter converter;

	@BeforeEach
	void beforeEach() {

		this.context = new MongoMappingContext();
		this.converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		this.converter.afterPropertiesSet();

		this.mapper = new AggregationMapper(converter);
	}

	@Test // GH-5058
	void mapsFieldReferenceUsingDomainTypeMapping() {

		Document aggregation = new Document("$group", new Document("_id", "$refId"));
		Document result = mapper.getMappedObject(aggregation, context.getRequiredPersistentEntity(WithReference.class));

		assertThat(result.get("$group", Document.class)).containsEntry("_id", "$ref_id");
	}

	@Test // GH-5058
	void doesNotMapFieldReferenceIfNoPropertyFound() {

		Document aggregation = new Document("$group", new Document("_id", "$unknown"));
		Document result = mapper.getMappedObject(aggregation, context.getRequiredPersistentEntity(WithReference.class));

		assertThat(result.get("$group", Document.class)).containsEntry("_id", "$unknown");
	}

	@Test // GH-5058
	void leavesSystemVariablesAsIs() {

		Document aggregation = new Document("$project", new Document("refId", "$$ROOT"));
		Document result = mapper.getMappedObject(aggregation, context.getRequiredPersistentEntity(WithReference.class));

		assertThat(result.get("$project", Document.class)).containsEntry("ref_id", "$$ROOT");
	}

	@Test // GH-5086
	void doesNotMapLiteralValue() {

		Document aggregation = new Document("$project", new Document("refId", new Document("$literal", "$refId")));
		Document result = mapper.getMappedObject(aggregation, context.getRequiredPersistentEntity(WithReference.class));

		assertThat(result.get("$project", Document.class)).containsEntry("ref_id", new Document("$literal", "$refId"));
	}

	@Test // GH-5011
	void ignoresInvalidPaths() {

		Document aggregation = new Document("missing.path", "value");
		Document result = mapper.getMappedObject(aggregation, context.getRequiredPersistentEntity(WithReference.class));

		assertThat(result).containsEntry("missing.path", "value");
	}

	static class WithReference {

		@Field("ref_id")
		ObjectId refId;
	}
}
