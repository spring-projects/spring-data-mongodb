/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;

/**
 * {@link IndexFilter} implementation for usage with plain {@link Document} as well as {@link CriteriaDefinition} filter
 * expressions.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class PartialIndexFilter implements IndexFilter {

	private final Object filterExpression;

	private PartialIndexFilter(Object filterExpression) {

		Assert.notNull(filterExpression, "FilterExpression must not be null");

		this.filterExpression = filterExpression;
	}

	/**
	 * Create new {@link PartialIndexFilter} for given {@link Document filter expression}.
	 *
	 * @param where must not be {@literal null}.
	 * @return
	 */
	public static PartialIndexFilter of(Document where) {
		return new PartialIndexFilter(where);
	}

	/**
	 * Create new {@link PartialIndexFilter} for given {@link CriteriaDefinition filter expression}.
	 *
	 * @param where must not be {@literal null}.
	 * @return
	 */
	public static PartialIndexFilter of(CriteriaDefinition where) {
		return new PartialIndexFilter(where);
	}

	public Document getFilterObject() {

		if (filterExpression instanceof Document document) {
			return document;
		}

		if (filterExpression instanceof CriteriaDefinition criteriaDefinition) {
			return criteriaDefinition.getCriteriaObject();
		}

		throw new IllegalArgumentException(
				String.format("Unknown type %s used as filter expression", filterExpression.getClass()));
	}
}
