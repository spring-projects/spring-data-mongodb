/*
 * Copyright 2018-2023 the original author or authors.
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
import java.util.Collection;
import java.util.Collections;

import org.bson.Document;
import org.springframework.util.Assert;

/**
 * Gateway for
 * <a href="https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#object-expression-operators">object
 * expression operators</a>.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class ObjectOperators {

	/**
	 * Take the value referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link ObjectOperatorFactory}.
	 */
	public static ObjectOperatorFactory valueOf(String fieldReference) {
		return new ObjectOperatorFactory(Fields.field(fieldReference));
	}

	/**
	 * Take the value provided by the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link ObjectOperatorFactory}.
	 */
	public static ObjectOperatorFactory valueOf(AggregationExpression expression) {
		return new ObjectOperatorFactory(expression);
	}

	/**
	 * Use the value from the given {@link SystemVariable} as input for the target {@link AggregationExpression expression}.
	 *
	 * @param variable the {@link SystemVariable} to use (eg. {@link SystemVariable#ROOT}.
	 * @return new instance of {@link ObjectOperatorFactory}.
	 * @since 4.2
	 */
	public static ObjectOperatorFactory valueOf(SystemVariable variable) {
		return new ObjectOperatorFactory(Fields.field(variable.getName(), variable.getTarget()));
	}

	/**
	 * Get the value of the field with given name from the {@literal $$CURRENT} object.
	 * Short version for {@code ObjectOperators.valueOf("$$CURRENT").getField(fieldName)}.
	 *
	 * @param fieldName the field name.
	 * @return new instance of {@link AggregationExpression}.
	 * @since 4.2
	 */
	public static AggregationExpression getValueOf(String fieldName) {
		return new ObjectOperatorFactory(SystemVariable.CURRENT).getField(fieldName);
	}

	/**
	 * Set the value of the field with given name on the {@literal $$CURRENT} object.
	 * Short version for {@code ObjectOperators.valueOf($$CURRENT).setField(fieldName).toValue(value)}.
	 *
	 * @param fieldName the field name.
	 * @return new instance of {@link AggregationExpression}.
	 * @since 4.2
	 */
	public static AggregationExpression setValueTo(String fieldName, Object value) {
		return new ObjectOperatorFactory(SystemVariable.CURRENT).setField(fieldName).toValue(value);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class ObjectOperatorFactory {

		private final Object value;

		/**
		 * Creates new {@link ObjectOperatorFactory} for given {@literal value}.
		 *
		 * @param value must not be {@literal null}.
		 */
		public ObjectOperatorFactory(Object value) {

			Assert.notNull(value, "Value must not be null");

			this.value = value;
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the associated value and uses
		 * {@literal $mergeObjects} as an accumulator within the {@literal $group} stage. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects merge() {
			return MergeObjects.merge(value);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the associated value and combines it with the
		 * given values (documents or mapped objects) into a single document. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWith(Object... values) {
			return merge().mergeWith(values);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the associated value and combines it with the
		 * values of the given {@link Field field references} into a single document. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWithValuesOf(String... fieldReferences) {
			return merge().mergeWithValuesOf(fieldReferences);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the associated value and combines it with the
		 * result values of the given {@link Aggregation expressions} into a single document. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWithValuesOf(AggregationExpression... expression) {
			return merge().mergeWithValuesOf(expression);
		}

		/**
		 * Creates new {@link ObjectToArray aggregation expression} that takes the associated value and converts it to an
		 * array of {@link Document documents} that contain two fields {@literal k} and {@literal v} each. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @since 2.1
		 */
		public ObjectToArray toArray() {
			return ObjectToArray.toArray(value);
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the associated value and obtains the value of the
		 * field with matching name.
		 *
		 * @since 4.0
		 */
		public GetField getField(String fieldName) {
			return GetField.getField(Fields.field(fieldName)).of(value);
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the associated value and obtains the value of the
		 * field with matching name.
		 *
		 * @since 4.0
		 */
		public SetField setField(String fieldName) {
			return SetField.field(Fields.field(fieldName)).input(value);
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the associated value and obtains the value of the
		 * field with matching name.
		 *
		 * @since 4.0
		 */
		public AggregationExpression removeField(String fieldName) {
			return SetField.field(fieldName).input(value).toValue(SystemVariable.REMOVE);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $mergeObjects} that combines multiple documents into a single document.
	 * <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/">https://docs.mongodb.com/manual/reference/operator/aggregation/mergeObjects/</a>
	 * @since 2.1
	 */
	public static class MergeObjects extends AbstractAggregationExpression {

		private MergeObjects(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes given values and combines them into a single
		 * document. <br />
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public static MergeObjects merge(Object... values) {
			return new MergeObjects(Arrays.asList(values));
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the given {@link Field field references} and
		 * combines them into a single document.
		 *
		 * @param fieldReferences must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public static MergeObjects mergeValuesOf(String... fieldReferences) {
			return merge(Arrays.stream(fieldReferences).map(Fields::field).toArray());
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} that takes the result of the given {@link Aggregation
		 * expressions} and combines them into a single document.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public static MergeObjects mergeValuesOf(AggregationExpression... expressions) {
			return merge(expressions);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} by adding the given {@link Field field references}.
		 *
		 * @param fieldReferences must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWithValuesOf(String... fieldReferences) {
			return mergeWith(Arrays.stream(fieldReferences).map(Fields::field).toArray());
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} by adding the given {@link AggregationExpression
		 * expressions}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWithValuesOf(AggregationExpression... expression) {
			return mergeWith(expression);
		}

		/**
		 * Creates new {@link MergeObjects aggregation expression} by adding the given values (documents or mapped objects).
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link MergeObjects}.
		 */
		public MergeObjects mergeWith(Object... values) {
			return new MergeObjects(append(Arrays.asList(values)));
		}

		@Override
		public Document toDocument(Object value, AggregationOperationContext context) {
			return super.toDocument(potentiallyExtractSingleValue(value), context);
		}

		@SuppressWarnings("unchecked")
		private Object potentiallyExtractSingleValue(Object value) {

			if (value instanceof Collection<?> collection && collection.size() == 1) {
				return collection.iterator().next();
			}
			return value;
		}

		@Override
		protected String getMongoMethod() {
			return "$mergeObjects";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $objectToArray} that converts a document to an array of {@link Document
	 * documents} that each contains two fields {@literal k} and {@literal v}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/objectToArray/">https://docs.mongodb.com/manual/reference/operator/aggregation/objectToArray/</a>
	 * @since 2.1
	 */
	public static class ObjectToArray extends AbstractAggregationExpression {

		private ObjectToArray(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ObjectToArray aggregation expression} that takes the value pointed to by given {@link Field
		 * fieldReference} and converts it to an array.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ObjectToArray}.
		 */
		public static ObjectToArray valueOfToArray(String fieldReference) {
			return toArray(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ObjectToArray aggregation expression} that takes the result value of the given
		 * {@link AggregationExpression expression} and converts it to an array.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ObjectToArray}.
		 */
		public static ObjectToArray valueOfToArray(AggregationExpression expression) {
			return toArray(expression);
		}

		/**
		 * Creates new {@link ObjectToArray aggregation expression} that takes the given value and converts it to an array.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ObjectToArray}.
		 */
		public static ObjectToArray toArray(Object value) {
			return new ObjectToArray(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$objectToArray";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $getField}.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class GetField extends AbstractAggregationExpression {

		protected GetField(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the value pointed to by given {@code fieldName}.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link GetField}.
		 */
		public static GetField getField(String fieldName) {
			return new GetField(Collections.singletonMap("field", fieldName));
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the value pointed to by given {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 * @return new instance of {@link GetField}.
		 */
		public static GetField getField(Field field) {
			return new GetField(Collections.singletonMap("field", field));
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the value pointed to by given
		 * {@code field reference}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return new instance of {@link GetField}.
		 */
		public GetField of(String fieldRef) {
			return of(Fields.field(fieldRef));
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the value pointed to by given
		 * {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link GetField}.
		 */
		public GetField of(AggregationExpression expression) {
			return of((Object) expression);
		}

		private GetField of(Object fieldRef) {
			return new GetField(append("input", fieldRef));
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {

			if(isArgumentMap() && get("field") instanceof Field field) {
				return new GetField(append("field", context.getReference(field).getRaw())).toDocument(context);
			}
			return super.toDocument(context);
		}

		@Override
		protected String getMongoMethod() {
			return "$getField";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setField}.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class SetField extends AbstractAggregationExpression {

		protected SetField(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the value pointed to by given input
		 * {@code fieldName}.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		public static SetField field(String fieldName) {
			return new SetField(Collections.singletonMap("field", fieldName));
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the value pointed to by given input {@link Field}.
		 *
		 * @param field must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		public static SetField field(Field field) {
			return new SetField(Collections.singletonMap("field", field));
		}

		/**
		 * Creates new {@link GetField aggregation expression} that takes the value pointed to by given input
		 * {@code field reference}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return new instance of {@link GetField}.
		 */
		public SetField input(String fieldRef) {
			return input(Fields.field(fieldRef));
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the value pointed to by given input
		 * {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		public SetField input(AggregationExpression expression) {
			return input((Object) expression);
		}

		/**
		 * Creates new {@link SetField aggregation expression} that takes the value pointed to by given input
		 * {@code field reference}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		private SetField input(Object fieldRef) {
			return new SetField(append("input", fieldRef));
		}

		/**
		 * Creates new {@link SetField aggregation expression} providing the {@code value} using {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		public SetField toValueOf(String fieldReference) {
			return toValue(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link SetField aggregation expression} providing the {@code value} using
		 * {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link SetField}.
		 */
		public SetField toValueOf(AggregationExpression expression) {
			return toValue(expression);
		}

		/**
		 * Creates new {@link SetField aggregation expression} providing the {@code value}.
		 *
		 * @param value
		 * @return new instance of {@link SetField}.
		 */
		public SetField toValue(Object value) {
			return new SetField(append("value", value));
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {
			if(get("field") instanceof Field field) {
				return new SetField(append("field", context.getReference(field).getRaw())).toDocument(context);
			}
			return super.toDocument(context);
		}

		@Override
		protected String getMongoMethod() {
			return "$setField";
		}
	}
}
