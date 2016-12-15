/*
 * Copyright 2016. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.aggregation;

import org.springframework.util.Assert;

/**
 * Gateway to {@literal literal} aggregation operations.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class LiteralOperators {

	/**
	 * Take the value referenced by given {@literal value}.
	 *
	 * @param value must not be {@literal null}.
	 * @return
	 */
	public static LiteralOperatorFactory valueOf(Object value) {

		Assert.notNull(value, "Value must not be null!");
		return new LiteralOperatorFactory(value);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class LiteralOperatorFactory {

		private final Object value;

		/**
		 * Creates new {@link LiteralOperatorFactory} for given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 */
		public LiteralOperatorFactory(Object value) {

			Assert.notNull(value, "Value must not be null!");
			this.value = value;
		}

		/**
		 * Creates new {@link AggregationExpressions} that returns the associated value without parsing.
		 *
		 * @return
		 */
		public Literal asLiteral() {
			return Literal.asLiteral(value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $literal}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Literal extends AbstractAggregationExpression {

		private Literal(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$literal";
		}

		/**
		 * Creates new {@link Literal}.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public static Literal asLiteral(Object value) {

			Assert.notNull(value, "Value must not be null!");
			return new Literal(value);
		}
	}
}
