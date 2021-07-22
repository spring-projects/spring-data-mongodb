/*
 * Copyright 2021 the original author or authors.
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

import org.bson.Document;

/**
 * @author Christoph Strobl
 * @since 3.3
 */
public class DocumentOperators {

	/**
	 * Obtain the document position (including gaps) relative to others (rank).
	 *
	 * @return new instance of {@link Rank}.
	 * @since 3.3
	 */
	public static Rank rank() {
		return new Rank();
	}

	/**
	 * Obtain the document position (without gaps) relative to others (rank).
	 *
	 * @return new instance of {@link DenseRank}.
	 * @since 3.3
	 */
	public static DenseRank denseRank() {
		return new DenseRank();
	}

	/**
	 * {@link Rank} resolves the current document position (the rank) relative to other documents. If multiple documents
	 * occupy the same rank, {@literal $rank} places the document with the subsequent value at a rank with a gap.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Rank implements AggregationExpression {

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document("$rank", new Document());
		}
	}

	/**
	 * {@link DenseRank} resolves the current document position (the rank) relative to other documents. If multiple
	 * documents occupy the same rank, {@literal $denseRank} places the document with the subsequent value at the next rank without
	 * any gaps.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class DenseRank implements AggregationExpression {

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document("$denseRank", new Document());
		}
	}
}
