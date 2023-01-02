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
package org.springframework.data.mongodb;

/**
 * Wrapper object for MongoDB expressions like {@code $toUpper : $name} that manifest as {@link org.bson.Document} when
 * passed on to the driver.
 * <br />
 * A set of predefined {@link MongoExpression expressions}, including a
 * {@link org.springframework.data.mongodb.core.aggregation.AggregationSpELExpression SpEL based variant} for method
 * like expressions (eg. {@code toUpper(name)}) are available via the
 * {@link org.springframework.data.mongodb.core.aggregation Aggregation API}.
 *
 * @author Christoph Strobl
 * @since 3.2
 * @see org.springframework.data.mongodb.core.aggregation.ArithmeticOperators
 * @see org.springframework.data.mongodb.core.aggregation.ArrayOperators
 * @see org.springframework.data.mongodb.core.aggregation.ComparisonOperators
 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperators
 * @see org.springframework.data.mongodb.core.aggregation.ConvertOperators
 * @see org.springframework.data.mongodb.core.aggregation.DateOperators
 * @see org.springframework.data.mongodb.core.aggregation.ObjectOperators
 * @see org.springframework.data.mongodb.core.aggregation.SetOperators
 * @see org.springframework.data.mongodb.core.aggregation.StringOperators
 */
@FunctionalInterface
public interface MongoExpression {

	/**
	 * Create a new {@link MongoExpression} from plain {@link String} (eg. {@code $toUpper : $name}). <br />
	 * The given expression will be wrapped with <code>{ ... }</code> to match an actual MongoDB {@link org.bson.Document}
	 * if necessary.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link MongoExpression}.
	 */
	static MongoExpression create(String expression) {
		return new BindableMongoExpression(expression, null);
	}

	/**
	 * Create a new {@link MongoExpression} from plain {@link String} containing placeholders (eg. {@code $toUpper : ?0})
	 * that will be resolved on first call of {@link #toDocument()}. <br />
	 * The given expression will be wrapped with <code>{ ... }</code> to match an actual MongoDB {@link org.bson.Document}
	 * if necessary.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link MongoExpression}.
	 */
	static MongoExpression create(String expression, Object... args) {
		return new BindableMongoExpression(expression, args);
	}

	/**
	 * Obtain the native {@link org.bson.Document} representation.
	 *
	 * @return never {@literal null}.
	 */
	org.bson.Document toDocument();
}
