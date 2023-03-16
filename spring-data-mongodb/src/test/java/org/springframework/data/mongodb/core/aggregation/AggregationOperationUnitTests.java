/*
 * Copyright 2023 the original author or authors.
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

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * @author Christoph Strobl
 */
class AggregationOperationUnitTests {

	@Test // GH-4306
	void getOperatorShouldFavorToPipelineStages() {

		AggregationOperation op = new AggregationOperation() {

			@Override
			public Document toDocument(AggregationOperationContext context) {
				return new Document();
			}

			@Override
			public List<Document> toPipelineStages(AggregationOperationContext context) {
				return List.of(new Document("$spring", "data"));
			}
		};

		assertThat(op.getOperator()).isEqualTo("$spring");
	}

}
