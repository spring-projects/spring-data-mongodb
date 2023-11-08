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
package org.springframework.data.mongodb.core.aggregation;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.lang.Nullable;

/**
 * Unit tests for {@link RedactOperation}.
 *
 * @author Christoph Strobl
 */
class RedactOperationUnitTests {

	Document expected = new Document("$redact",
			new Document("$cond", new Document("if", new Document("$eq", Arrays.asList("$level", 5)))
					.append("then", "$$PRUNE").append("else", "$$DESCEND")));
	Document expectedMapped = new Document("$redact",
			new Document("$cond", new Document("if", new Document("$eq", Arrays.asList("$le_v_el", 5)))
					.append("then", "$$PRUNE").append("else", "$$DESCEND")));

	@Test // DATAMONGO-931
	void errorsOnNullExpression() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> new RedactOperation(null));
	}

	@Test // DATAMONGO-931
	void mapsAggregationExpressionCorrectly() {

		assertThat(new RedactOperation(ConditionalOperators.when(Criteria.where("level").is(5)) //
				.then(RedactOperation.PRUNE) //
				.otherwise(RedactOperation.DESCEND)).toDocument(contextFor(null))).isEqualTo(expected);
	}

	@Test // DATAMONGO-931
	void mapsAggregationExpressionViaBuilderCorrectly() {

		assertThat(RedactOperation.builder().when(Criteria.where("level").is(5)) //
				.thenPrune() //
				.otherwiseDescend().build().toDocument(contextFor(null))).isEqualTo(expected);
	}

	@Test // DATAMONGO-931
	void mapsTypedAggregationExpressionCorrectly() {

		assertThat(new RedactOperation(ConditionalOperators.when(Criteria.where("level").is(5)) //
				.then(RedactOperation.PRUNE) //
				.otherwise(RedactOperation.DESCEND)).toDocument(contextFor(DomainType.class))).isEqualTo(expectedMapped);
	}

	static class DomainType {

		@Field("le_v_el") String level;

		public String getLevel() {
			return this.level;
		}

		public void setLevel(String level) {
			this.level = level;
		}

		public String toString() {
			return "RedactOperationUnitTests.DomainType(level=" + this.getLevel() + ")";
		}
	}

	private static AggregationOperationContext contextFor(@Nullable Class<?> type) {

		if (type == null) {
			return Aggregation.DEFAULT_CONTEXT;
		}

		MappingMongoConverter mongoConverter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE,
				new MongoMappingContext());
		mongoConverter.afterPropertiesSet();

		return new TypeBasedAggregationOperationContext(type, mongoConverter.getMappingContext(),
				new QueryMapper(mongoConverter)).continueOnMissingFieldReference();
	}
}
