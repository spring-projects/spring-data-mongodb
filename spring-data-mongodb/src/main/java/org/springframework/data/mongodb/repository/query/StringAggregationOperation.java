/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperationContext;
import org.springframework.lang.Nullable;

/**
 * String-based aggregation operation for a repository query method.
 *
 * @author Christoph Strobl
 * @since 4.3.1
 */
class StringAggregationOperation implements AggregationOperation {

	private static final Pattern OPERATOR_PATTERN = Pattern.compile("\\$\\w+");

	private final String source;
	private final Class<?> domainType;
	private final Function<String, Document> bindFunction;
	private final @Nullable String operator;

	StringAggregationOperation(String source, Class<?> domainType, Function<String, Document> bindFunction) {

		this.source = source;
		this.domainType = domainType;
		this.bindFunction = bindFunction;

		Matcher matcher = OPERATOR_PATTERN.matcher(source);
		this.operator = matcher.find() ? matcher.group() : null;
	}

	@Override
	public Document toDocument(AggregationOperationContext context) {
		return context.getMappedObject(bindFunction.apply(source), domainType);
	}

	@Override
	public String getOperator() {
		return operator != null ? operator : AggregationOperation.super.getOperator();
	}
}
