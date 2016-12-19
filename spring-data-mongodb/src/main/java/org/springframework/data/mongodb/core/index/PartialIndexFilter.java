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
package org.springframework.data.mongodb.core.index;

import org.bson.Document;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import org.springframework.data.mongodb.core.query.CriteriaDefinition;

import com.mongodb.DBObject;

/**
 * {@link IndexFilter} implementation for usage with plain {@link DBObject} as well as {@link CriteriaDefinition} filter
 * expressions.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class PartialIndexFilter implements IndexFilter {

	private final @NonNull Object filterExpression;

	/**
	 * Create new {@link PartialIndexFilter} for given {@link DBObject filter expression}.
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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexFilter#getFilterObject()
	 */
	public Document getFilterObject() {

		if (filterExpression instanceof Document) {
			return (Document) filterExpression;
		}

		if (filterExpression instanceof CriteriaDefinition) {
			return ((CriteriaDefinition) filterExpression).getCriteriaObject();
		}

		throw new IllegalArgumentException(
				String.format("Unknown type %s used as filter expression.", filterExpression.getClass()));
	}
}
