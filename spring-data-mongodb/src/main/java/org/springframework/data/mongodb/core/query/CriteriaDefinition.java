/*
 * Copyright 2010-2025 the original author or authors.
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
package org.springframework.data.mongodb.core.query;

import org.bson.Document;
import org.jspecify.annotations.Nullable;

/**
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public interface CriteriaDefinition {

	/**
	 * Get {@link Document} representation.
	 *
	 * @return never {@literal null}.
	 */
	Document getCriteriaObject();

	/**
	 * Get the identifying {@literal key}.
	 *
	 * @return can be {@literal null}.
	 * @since 1.6
	 */
	@Nullable
	String getKey();

	/**
	 * A placeholder expression used when rending queries to JSON.
	 *
	 * @since 5.0
	 * @author Christoph Strobl
	 */
	interface Placeholder {

		/**
		 * Create a new placeholder for index bindable parameter.
		 * 
		 * @param position the index of the parameter to bind.
		 * @return new instance of {@link Placeholder}.
		 */
		static Placeholder indexed(int position) {
			return new PlaceholderImpl("?%s".formatted(position));
		}

		static Placeholder placeholder(String expression) {
			return new PlaceholderImpl(expression);
		}

		Object getValue();
	}

	static class PlaceholderImpl implements Placeholder {
		private final Object expression;

		public PlaceholderImpl(Object expression) {
			this.expression = expression;
		}

		@Override
		public Object getValue() {
			return expression;
		}

		public String toString() {
			return getValue().toString();
		}
	}
}
