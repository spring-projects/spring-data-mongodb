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

import org.springframework.lang.Nullable;

/**
 * Describes the system variables available in MongoDB aggregation framework pipeline expressions.
 *
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @see <a href="https://docs.mongodb.com/manual/reference/aggregation-variables">Aggregation Variables</a>.
 */
public enum SystemVariable implements AggregationVariable {

	/**
	 * Variable for the current datetime.
	 *
	 * @since 4.0
	 */
	NOW,

	/**
	 * Variable for the current timestamp.
	 *
	 * @since 4.0
	 */
	CLUSTER_TIME,

	/**
	 * Variable that references the root document.
	 */
	ROOT,

	/**
	 * Variable that references the start of the field path being processed.
	 */
	CURRENT,

	/**
	 * Variable that evaluates to a missing value.
	 */
	REMOVE,

	/**
	 * One of the allowed results of a {@literal $redact} expression
	 *
	 * @since 4.0
	 */
	DESCEND,

	/**
	 * One of the allowed results of a {@literal $redact} expression
	 *
	 * @since 4.0
	 */
	PRUNE,
	/**
	 * One of the allowed results of a {@literal $redact} expression
	 *
	 * @since 4.0
	 */
	KEEP,

	/**
	 * A variable that stores the metadata results of an Atlas Search query.
	 *
	 * @since 4.0
	 */
	SEARCH_META;

	/**
	 * Return {@literal true} if the given {@code fieldRef} denotes a well-known system variable, {@literal false}
	 * otherwise.
	 *
	 * @param fieldRef may be {@literal null}.
	 * @return {@literal true} if the given field refers to a {@link SystemVariable}.
	 */
	public static boolean isReferingToSystemVariable(@Nullable String fieldRef) {

		String candidate = variableNameFrom(fieldRef);
		if (candidate == null) {
			return false;
		}

		candidate = candidate.startsWith(PREFIX) ? candidate.substring(2) : candidate;
		for (SystemVariable value : values()) {
			if (value.name().equals(candidate)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public String toString() {
		return PREFIX.concat(name());
	}

	@Override
	public String getTarget() {
		return toString();
	}

	@Nullable
	static String variableNameFrom(@Nullable String fieldRef) {

		if (fieldRef == null || !fieldRef.startsWith(PREFIX) || fieldRef.length() <= 2) {
			return null;
		}

		int indexOfFirstDot = fieldRef.indexOf('.');
		return indexOfFirstDot == -1 ? fieldRef : fieldRef.substring(2, indexOfFirstDot);
	}
}
