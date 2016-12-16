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
 * Gateway to {@literal data type} expressions.
 *
 * @author Christoph Strobl
 * @since 1.10
 * @soundtrack Clawfinger - Catch Me
 */
public class DataTypeOperators {

	/**
	 * Return the BSON data type of the given {@literal field}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static Type typeOf(String fieldReference) {
		return Type.typeOf(fieldReference);
	}

	/**
	 * {@link AggregationExpression} for {@code $type}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Type extends AbstractAggregationExpression {

		private Type(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$type";
		}

		/**
		 * Creates new {@link Type}.
		 *
		 * @param field must not be {@literal null}.
		 * @return
		 */
		public static Type typeOf(String field) {

			Assert.notNull(field, "Field must not be null!");
			return new Type(Fields.field(field));
		}
	}
}
