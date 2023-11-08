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

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond.ThenBuilder;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;

/**
 * {@link RedactOperation} allows to restrict the content of a {@link Document} based on information stored within
 * itself.
 *
 * <pre class="code">
 * RedactOperation.builder() //
 * 		.when(Criteria.where("level").is(5)) //
 * 		.thenPrune() //
 * 		.otherwiseDescend() //
 * 		.build();
 * </pre>
 *
 * @author Christoph Strobl
 * @see <a href="https://docs.mongodb.com/manual/reference/operator/aggregation/redact/">https://docs.mongodb.com/manual/reference/operator/aggregation/redact/</a>
 * @since 3.0
 */
public class RedactOperation implements AggregationOperation {

	/**
	 * Return fields at the current document level. Exclude embedded ones.
	 */
	public static final String DESCEND = "$$DESCEND";

	/**
	 * Return/Keep all fields at the current document/embedded level.
	 */
	public static final String KEEP = "$$KEEP";

	/**
	 * Exclude all fields at this current document/embedded level.
	 */
	public static final String PRUNE = "$$PRUNE";

	private final AggregationExpression condition;

	/**
	 * Create new {@link RedactOperation}.
	 *
	 * @param condition Any {@link AggregationExpression} that resolves to {@literal $$DESCEND}, {@literal $$PRUNE}, or
	 *          {@literal $$KEEP}. Must not be {@literal null}.
	 */
	public RedactOperation(AggregationExpression condition) {

		Assert.notNull(condition, "Condition must not be null");
		this.condition = condition;
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {
		return new Document(getOperator(), condition.toDocument(context));
	}

	@Override
	public String getOperator() {
		return "$redact";
	}

	/**
	 * Obtain a new instance of {@link RedactOperationBuilder} to specify condition and outcome of the {@literal $redact}
	 * operation.
	 *
	 * @return new instance of {@link RedactOperationBuilder}.
	 */
	public static RedactOperationBuilder builder() {
		return new RedactOperationBuilder();
	}

	/**
	 * Builder to create new instance of {@link RedactOperation}.
	 *
	 * @author Christoph Strobl
	 */
	public static class RedactOperationBuilder {

		private Object when;
		private Object then;
		private Object otherwise;

		private RedactOperationBuilder() {

		}

		/**
		 * Specify the evaluation condition.
		 *
		 * @param criteria must not be {@literal null}.
		 * @return this.
		 */
		public RedactOperationBuilder when(CriteriaDefinition criteria) {

			this.when = criteria;
			return this;
		}

		/**
		 * Specify the evaluation condition.
		 *
		 * @param condition must not be {@literal null}.
		 * @return this.
		 */
		public RedactOperationBuilder when(AggregationExpression condition) {

			this.when = condition;
			return this;
		}

		/**
		 * Specify the evaluation condition.
		 *
		 * @param condition must not be {@literal null}.
		 * @return this.
		 */
		public RedactOperationBuilder when(Document condition) {

			this.when = condition;
			return this;
		}

		/**
		 * Return fields at the current document level and exclude embedded ones if the condition is met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder thenDescend() {
			return then(DESCEND);
		}

		/**
		 * Return/Keep all fields at the current document/embedded level if the condition is met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder thenKeep() {
			return then(KEEP);
		}

		/**
		 * Exclude all fields at this current document/embedded level if the condition is met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder thenPrune() {
			return then(PRUNE);
		}

		/**
		 * Define the outcome (anything that resolves to {@literal $$DESCEND}, {@literal $$PRUNE}, or {@literal $$KEEP})
		 * when the condition is met.
		 *
		 * @param then must not be {@literal null}.
		 * @return this.
		 */
		public RedactOperationBuilder then(Object then) {

			this.then = then;
			return this;
		}

		/**
		 * Return fields at the current document level and exclude embedded ones if the condition is not met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder otherwiseDescend() {
			return otherwise(DESCEND);
		}

		/**
		 * Return/Keep all fields at the current document/embedded level if the condition is not met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder otherwiseKeep() {
			return otherwise(KEEP);
		}

		/**
		 * Exclude all fields at this current document/embedded level if the condition is not met.
		 *
		 * @return this.
		 */
		public RedactOperationBuilder otherwisePrune() {
			return otherwise(PRUNE);
		}

		/**
		 * Define the outcome (anything that resolves to {@literal $$DESCEND}, {@literal $$PRUNE}, or {@literal $$KEEP})
		 * when the condition is not met.
		 *
		 * @param otherwise must not be {@literal null}.
		 * @return this.
		 */
		public RedactOperationBuilder otherwise(Object otherwise) {
			this.otherwise = otherwise;
			return this;
		}

		/**
		 * @return new instance of {@link RedactOperation}.
		 */
		public RedactOperation build() {
			return new RedactOperation(when().then(then).otherwise(otherwise));
		}

		private ThenBuilder when() {

			if (when instanceof CriteriaDefinition criteriaDefinition) {
				return ConditionalOperators.Cond.when(criteriaDefinition);
			}
			if (when instanceof AggregationExpression aggregationExpression) {
				return ConditionalOperators.Cond.when(aggregationExpression);
			}
			if (when instanceof Document document) {
				return ConditionalOperators.Cond.when(document);
			}

			throw new IllegalArgumentException(String.format(
					"Invalid Condition; Expected CriteriaDefinition, AggregationExpression or Document but was %s", when));
		}
	}
}
