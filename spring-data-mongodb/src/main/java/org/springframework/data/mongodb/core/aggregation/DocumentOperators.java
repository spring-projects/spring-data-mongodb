/*
 * Copyright 2021-2023 the original author or authors.
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

import java.util.Collections;

import org.bson.Document;

/**
 * Gateway to {@literal document expressions} such as {@literal $rank, $documentNumber, etc.}
 *
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
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link DocumentOperatorsFactory}.
	 */
	public static DocumentOperatorsFactory valueOf(String fieldReference) {
		return new DocumentOperatorsFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link DocumentOperatorsFactory}.
	 */
	public static DocumentOperatorsFactory valueOf(AggregationExpression expression) {
		return new DocumentOperatorsFactory(expression);
	}

	/**
	 * Obtain the current document position.
	 *
	 * @return new instance of {@link DocumentNumber}.
	 * @since 3.3
	 */
	public static DocumentNumber documentNumber() {
		return new DocumentNumber();
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class DocumentOperatorsFactory {

		private final Object target;

		public DocumentOperatorsFactory(Object target) {
			this.target = target;
		}

		/**
		 * Creates new {@link AggregationExpression} that applies the expression to a document at specified position
		 * relative to the current document.
		 *
		 * @param by the value to add to the current position.
		 * @return new instance of {@link Shift}.
		 */
		public Shift shift(int by) {

			Shift shift = usesExpression() ? Shift.shift((AggregationExpression) target) : Shift.shift(target.toString());
			return shift.by(by);
		}

		private boolean usesExpression() {
			return target instanceof AggregationExpression;
		}
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
	 * documents occupy the same rank, {@literal $denseRank} places the document with the subsequent value at the next
	 * rank without any gaps.
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

	/**
	 * {@link DocumentNumber} resolves the current document position.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class DocumentNumber implements AggregationExpression {

		@Override
		public Document toDocument(AggregationOperationContext context) {
			return new Document("$documentNumber", new Document());
		}
	}

	/**
	 * Shift applies an expression to a document in a specified position relative to the current document.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class Shift extends AbstractAggregationExpression {

		private Shift(Object value) {
			super(value);
		}

		/**
		 * Specifies the field to evaluate and return.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Shift}.
		 */
		public static Shift shift(String fieldReference) {
			return new Shift(Collections.singletonMap("output", Fields.field(fieldReference)));
		}

		/**
		 * Specifies the {@link AggregationExpression expression} to evaluate and return.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Shift}.
		 */
		public static Shift shift(AggregationExpression expression) {
			return new Shift(Collections.singletonMap("output", expression));
		}

		/**
		 * Shift the document position relative to the current. Use a positive value for follow up documents (eg. 1 for the
		 * next) or a negative value for the predecessor documents (eg. -1 for the previous).
		 *
		 * @param shiftBy value to add to the current position.
		 * @return new instance of {@link Shift}.
		 */
		public Shift by(int shiftBy) {
			return new Shift(append("by", shiftBy));
		}

		/**
		 * Define the default value if the target document is out of range.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Shift}.
		 */
		public Shift defaultTo(Object value) {
			return new Shift(append("default", value));
		}

		/**
		 * Define the {@link AggregationExpression expression} to evaluate if the target document is out of range.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Shift}.
		 */
		public Shift defaultToValueOf(AggregationExpression expression) {
			return defaultTo(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$shift";
		}
	}
}
