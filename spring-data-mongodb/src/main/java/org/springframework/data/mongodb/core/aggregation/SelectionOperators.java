/*
 * Copyright 2022 the original author or authors.
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

import java.util.Arrays;
import java.util.Collections;

import org.springframework.data.domain.Sort;

/**
 * Gateway to {@literal selection operators} such as {@literal $bottom}.
 *
 * @author Christoph Strobl
 * @since 4.0
 */
public class SelectionOperators {

	/**
	 * {@link AbstractAggregationExpression} to return the bottom element according to the specified {@link #sortBy(Sort)
	 * order}.
	 */
	public static class Bottom extends AbstractAggregationExpression {

		private Bottom(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$bottom";
		}

		/**
		 * @return new instance of {@link Bottom}.
		 */
		public static Bottom bottom() {
			return new Bottom(Collections.emptyMap());
		}

		/**
		 * Define result ordering.
		 *
		 * @param sort must not be {@literal null}.
		 * @return new instance of {@link Bottom}.
		 */
		public Bottom sortBy(Sort sort) {
			return new Bottom(append("sortBy", sort));
		}

		/**
		 * Define result ordering.
		 *
		 * @param out must not be {@literal null}.
		 * @return new instance of {@link Bottom}.
		 */
		public Bottom output(Fields out) {
			return new Bottom(append("output", out));
		}

		/**
		 * Define fields included in the output for each element.
		 *
		 * @param fieldNames must not be {@literal null}.
		 * @return new instance of {@link Bottom}.
		 * @see #output(Fields)
		 */
		public Bottom output(String... fieldNames) {
			return output(Fields.fields(fieldNames));
		}

		/**
		 * Define expressions building the value included in the output for each element.
		 *
		 * @param out must not be {@literal null}.
		 * @return new instance of {@link Bottom}.
		 * @see #output(Fields)
		 */
		public Bottom output(AggregationExpression... out) {
			return new Bottom(append("output", Arrays.asList(out)));
		}
	}
}
