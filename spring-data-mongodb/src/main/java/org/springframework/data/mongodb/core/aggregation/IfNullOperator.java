/*
 * Copyright 2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Encapsulates the aggregation framework {@code $ifNull} operator. Replacement values can be either {@link Field field
 * references}, values of simple MongoDB types or values that can be converted to a simple MongoDB type.
 *
 * @see http://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public class IfNullOperator implements AggregationExpression {

	private final Field field;
	private final Object value;

	/**
	 * Creates a new {@link IfNullOperator} for the given {@link Field} and replacement {@code value}.
	 *
	 * @param field must not be {@literal null}.
	 * @param value must not be {@literal null}.
	 */
	public IfNullOperator(Field field, Object value) {

		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(value, "'Replacement-value' must not be null!");

		this.field = field;
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDocument(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		List<Object> list = new ArrayList<Object>();

		list.add(context.getReference(field).toString());
		list.add(resolve(value, context));

		return new Document("$ifNull", list);
	}

	private Object resolve(Object value, AggregationOperationContext context) {

		if (value instanceof Field) {
			return context.getReference((Field) value).toString();
		} else if (value instanceof Document) {
			return value;
		}

		return context.getMappedObject(new Document("$set", value)).get("$set");
	}

	/**
	 * Get a builder that allows fluent creation of {@link IfNullOperator}.
	 *
	 * @return a new {@link IfNullBuilder}.
	 */
	public static IfNullBuilder newBuilder() {
		return IfNullOperatorBuilder.newBuilder();
	}

	/**
	 * @since 1.10
	 */
	public static interface IfNullBuilder {

		/**
		 * @param field the field to check for a {@literal null} value, field reference must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder ifNull(Field field);

		/**
		 * @param field the field to check for a {@literal null} value, field name must not be {@literal null} or empty.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder ifNull(String field);
	}

	/**
	 * @since 1.10
	 */
	public static interface ThenBuilder {

		/**
		 * @param field the field holding the replacement value, must not be {@literal null}.
		 * @return the {@link IfNullOperator}
		 */
		IfNullOperator thenReplaceWith(Field field);

		/**
		 * @param value the value to be used if the {@code $ifNull }condition evaluates {@literal true}. Can be a
		 *          {@link Document}, a value that is supported by MongoDB or a value that can be converted to a MongoDB
		 *          representation but must not be {@literal null}.
		 * @return the {@link IfNullOperator}
		 */
		IfNullOperator thenReplaceWith(Object value);
	}

	/**
	 * Builder for fluent {@link IfNullOperator} creation.
	 *
	 * @author Mark Paluch
	 * @since 1.10
	 */
	public static final class IfNullOperatorBuilder implements IfNullBuilder, ThenBuilder {

		private Field field;

		private IfNullOperatorBuilder() {}

		/**
		 * Creates a new builder for {@link IfNullOperator}.
		 *
		 * @return never {@literal null}.
		 */
		public static IfNullOperatorBuilder newBuilder() {
			return new IfNullOperatorBuilder();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.IfNullOperator.IfNullBuilder#ifNull(org.springframework.data.mongodb.core.aggregation.Field)
		 */
		public ThenBuilder ifNull(Field field) {

			Assert.notNull(field, "Field must not be null!");

			this.field = field;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.IfNullOperator.IfNullBuilder#ifNull(java.lang.String)
		 */
		public ThenBuilder ifNull(String name) {

			Assert.hasText(name, "Field name must not be null or empty!");

			this.field = Fields.field(name);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.IfNullOperator.ThenReplaceBuilder#thenReplaceWith(org.springframework.data.mongodb.core.aggregation.Field)
		 */
		@Override
		public IfNullOperator thenReplaceWith(Field replacementField) {

			Assert.notNull(replacementField, "Replacement field must not be null!");

			return new IfNullOperator(this.field, replacementField);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.IfNullOperator.ThenReplaceBuilder#thenReplaceWith(java.lang.Object)
		 */
		@Override
		public IfNullOperator thenReplaceWith(Object value) {

			Assert.notNull(value, "'Replacement-value' must not be null!");

			return new IfNullOperator(this.field, value);
		}
	}
}
