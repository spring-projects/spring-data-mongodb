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

import static org.springframework.data.mongodb.test.util.Assertions.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * @author Christoph Strobl
 */
class MultiOperationAggregationStageUnitTests {

	@Test // GH-4306
	void toDocumentRendersSingleOperation() {

		MultiOperationAggregationStage stage = (ctx) -> List.of(Document.parse("{ $text: { $search: 'operating' } }"));

		assertThat(stage.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("{ $text: { $search: 'operating' } }");
	}

	@Test // GH-4306
	void toDocumentRendersMultiOperation() {

		MultiOperationAggregationStage stage = (ctx) -> List.of(Document.parse("{ $text: { $search: 'operating' } }"),
				Document.parse("{ $sort: { score: { $meta: 'textScore' } } }"));

		assertThat(stage.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("""
				{
					$text: { $search: 'operating' },
					$sort: { score: { $meta: 'textScore' } }
				}
				""");
	}

	@Test // GH-4306
	void toDocumentCollectsDuplicateOperation() {

		MultiOperationAggregationStage stage = (ctx) -> List.of(Document.parse("{ $text: { $search: 'operating' } }"),
				Document.parse("{ $sort: { score: { $meta: 'textScore' } } }"), Document.parse("{ $sort: { posts: -1 } }"));

		assertThat(stage.toDocument(Aggregation.DEFAULT_CONTEXT)).isEqualTo("""
				{
					$text: { $search: 'operating' },
					$sort: [
						{ score: { $meta: 'textScore' } },
						{ posts: -1 }
					]
				}
				""");
	}
}
