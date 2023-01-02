/*
 * Copyright 2022-2023 the original author or authors.
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

		/**
		 * In case a limit value ({@literal n}) is present {@literal $bottomN} is used instead of {@literal $bottom}.
		 *
		 * @return
		 */
		@Override
		protected String getMongoMethod() {
			return get("n") == null ? "$bottom" : "$bottomN";
		}

		/**
		 * @return new instance of {@link Bottom}.
		 */
		public static Bottom bottom() {
			return new Bottom(Collections.emptyMap());
		}

		/**
		 * @param numberOfResults Limits the number of returned elements to the given value.
		 * @return new instance of {@link Bottom}.
		 */
		public static Bottom bottom(int numberOfResults) {
			return bottom().limit(numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the given value.
		 *
		 * @param numberOfResults
		 * @return new instance of {@link Bottom}.
		 */
		public Bottom limit(int numberOfResults) {
			return limit((Object) numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the value defined by the given {@link AggregationExpression
		 * expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Bottom}.
		 */
		public Bottom limit(AggregationExpression expression) {
			return limit((Object) expression);
		}

		private Bottom limit(Object value) {
			return new Bottom(append("n", value));
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

	/**
	 * {@link AbstractAggregationExpression} to return the top element according to the specified {@link #sortBy(Sort)
	 * order}.
	 */
	public static class Top extends AbstractAggregationExpression {

		private Top(Object value) {
			super(value);
		}

		/**
		 * In case a limit value ({@literal n}) is present {@literal $topN} is used instead of {@literal $top}.
		 *
		 * @return
		 */
		@Override
		protected String getMongoMethod() {
			return get("n") == null ? "$top" : "$topN";
		}

		/**
		 * @return new instance of {@link Top}.
		 */
		public static Top top() {
			return new Top(Collections.emptyMap());
		}

		/**
		 * @param numberOfResults Limits the number of returned elements to the given value.
		 * @return new instance of {@link Top}.
		 */
		public static Top top(int numberOfResults) {
			return top().limit(numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the given value.
		 *
		 * @param numberOfResults
		 * @return new instance of {@link Top}.
		 */
		public Top limit(int numberOfResults) {
			return limit((Object) numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the value defined by the given {@link AggregationExpression
		 * expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Top}.
		 */
		public Top limit(AggregationExpression expression) {
			return limit((Object) expression);
		}

		private Top limit(Object value) {
			return new Top(append("n", value));
		}

		/**
		 * Define result ordering.
		 *
		 * @param sort must not be {@literal null}.
		 * @return new instance of {@link Top}.
		 */
		public Top sortBy(Sort sort) {
			return new Top(append("sortBy", sort));
		}

		/**
		 * Define result ordering.
		 *
		 * @param out must not be {@literal null}.
		 * @return new instance of {@link Top}.
		 */
		public Top output(Fields out) {
			return new Top(append("output", out));
		}

		/**
		 * Define fields included in the output for each element.
		 *
		 * @param fieldNames must not be {@literal null}.
		 * @return new instance of {@link Top}.
		 * @see #output(Fields)
		 */
		public Top output(String... fieldNames) {
			return output(Fields.fields(fieldNames));
		}

		/**
		 * Define expressions building the value included in the output for each element.
		 *
		 * @param out must not be {@literal null}.
		 * @return new instance of {@link Top}.
		 * @see #output(Fields)
		 */
		public Top output(AggregationExpression... out) {
			return new Top(append("output", Arrays.asList(out)));
		}
	}

	/**
	 * {@link AbstractAggregationExpression} to return the {@literal $firstN} elements.
	 */
	public static class First extends AbstractAggregationExpression {

		protected First(Object value) {
			super(value);
		}

		/**
		 * @return new instance of {@link First}.
		 */
		public static First first() {
			return new First(Collections.emptyMap());
		}

		/**
		 * @return new instance of {@link First}.
		 */
		public static First first(int numberOfResults) {
			return first().limit(numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the given value.
		 *
		 * @param numberOfResults
		 * @return new instance of {@link First}.
		 */
		public First limit(int numberOfResults) {
			return limit((Object) numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the value defined by the given {@link AggregationExpression
		 * expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link First}.
		 */
		public First limit(AggregationExpression expression) {
			return limit((Object) expression);
		}

		private First limit(Object value) {
			return new First(append("n", value));
		}

		/**
		 * Define the field to serve as source.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link First}.
		 */
		public First of(String fieldName) {
			return input(fieldName);
		}

		/**
		 * Define the expression building the value to serve as source.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link First}.
		 */
		public First of(AggregationExpression expression) {
			return input(expression);
		}

		/**
		 * Define the field to serve as source.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link First}.
		 */
		public First input(String fieldName) {
			return new First(append("input", Fields.field(fieldName)));
		}

		/**
		 * Define the expression building the value to serve as source.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link First}.
		 */
		public First input(AggregationExpression expression) {
			return new First(append("input", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$firstN";
		}
	}

	/**
	 * {@link AbstractAggregationExpression} to return the {@literal $lastN} elements.
	 */
	public static class Last extends AbstractAggregationExpression {

		protected Last(Object value) {
			super(value);
		}

		/**
		 * @return new instance of {@link Last}.
		 */
		public static Last last() {
			return new Last(Collections.emptyMap());
		}

		/**
		 * @return new instance of {@link Last}.
		 */
		public static Last last(int numberOfResults) {
			return last().limit(numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the given value.
		 *
		 * @param numberOfResults
		 * @return new instance of {@link Last}.
		 */
		public Last limit(int numberOfResults) {
			return limit((Object) numberOfResults);
		}

		/**
		 * Limits the number of returned elements to the value defined by the given {@link AggregationExpression
		 * expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Last}.
		 */
		public Last limit(AggregationExpression expression) {
			return limit((Object) expression);
		}

		private Last limit(Object value) {
			return new Last(append("n", value));
		}

		/**
		 * Define the field to serve as source.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link Last}.
		 */
		public Last of(String fieldName) {
			return input(fieldName);
		}

		/**
		 * Define the expression building the value to serve as source.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Last}.
		 */
		public Last of(AggregationExpression expression) {
			return input(expression);
		}

		/**
		 * Define the field to serve as source.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link Last}.
		 */
		public Last input(String fieldName) {
			return new Last(append("input", Fields.field(fieldName)));
		}

		/**
		 * Define the expression building the value to serve as source.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Last}.
		 */
		public Last input(AggregationExpression expression) {
			return new Last(append("input", expression));
		}

		@Override
		protected String getMongoMethod() {
			return "$lastN";
		}
	}
}
