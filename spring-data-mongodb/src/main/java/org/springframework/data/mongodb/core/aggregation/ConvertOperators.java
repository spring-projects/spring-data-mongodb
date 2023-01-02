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

import java.util.Collections;

import org.springframework.data.mongodb.core.schema.JsonSchemaObject.Type;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal convert} aggregation operations.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class ConvertOperators {

	/**
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static ConvertOperatorFactory valueOf(String fieldReference) {
		return new ConvertOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static ConvertOperatorFactory valueOf(AggregationExpression expression) {
		return new ConvertOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class ConvertOperatorFactory {

		private final @Nullable String fieldReference;
		private final @Nullable AggregationExpression expression;

		/**
		 * Creates new {@link ConvertOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public ConvertOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link ConvertOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ConvertOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given {@code stringTypeIdentifier}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param stringTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(String stringTypeIdentifier) {
			return createConvert().to(stringTypeIdentifier);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given {@code numericTypeIdentifier}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param numericTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(int numericTypeIdentifier) {
			return createConvert().to(numericTypeIdentifier);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given {@link Type}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertTo(Type type) {
			return createConvert().to(type);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the value of the given {@link Field field reference}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertToTypeOf(String fieldReference) {
			return createConvert().toTypeOf(fieldReference);
		}

		/**
		 * Creates new {@link Convert aggregation expression} that takes the associated value and converts it into the type
		 * specified by the given {@link AggregationExpression expression}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert convertToTypeOf(AggregationExpression expression) {
			return createConvert().toTypeOf(expression);
		}

		/**
		 * Creates new {@link ToBool aggregation expression} for {@code $toBool} that converts a value to boolean. Shorthand
		 * for {@link #convertTo(String) #convertTo("bool")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToBool}.
		 */
		public ToBool convertToBoolean() {
			return ToBool.toBoolean(valueObject());
		}

		/**
		 * Creates new {@link ToDate aggregation expression} for {@code $toDate} that converts a value to a date. Shorthand
		 * for {@link #convertTo(String) #convertTo("date")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToDate}.
		 */
		public ToDate convertToDate() {
			return ToDate.toDate(valueObject());
		}

		/**
		 * Creates new {@link ToDecimal aggregation expression} for {@code $toDecimal} that converts a value to a decimal.
		 * Shorthand for {@link #convertTo(String) #convertTo("decimal")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToDecimal}.
		 */
		public ToDecimal convertToDecimal() {
			return ToDecimal.toDecimal(valueObject());
		}

		/**
		 * Creates new {@link ToDouble aggregation expression} for {@code $toDouble} that converts a value to a decimal.
		 * Shorthand for {@link #convertTo(String) #convertTo("double")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToDouble}.
		 */
		public ToDouble convertToDouble() {
			return ToDouble.toDouble(valueObject());
		}

		/**
		 * Creates new {@link ToInt aggregation expression} for {@code $toInt} that converts a value to an int. Shorthand
		 * for {@link #convertTo(String) #convertTo("int")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToInt}.
		 */
		public ToInt convertToInt() {
			return ToInt.toInt(valueObject());
		}

		/**
		 * Creates new {@link ToInt aggregation expression} for {@code $toLong} that converts a value to a long. Shorthand
		 * for {@link #convertTo(String) #convertTo("long")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToInt}.
		 */
		public ToLong convertToLong() {
			return ToLong.toLong(valueObject());
		}

		/**
		 * Creates new {@link ToInt aggregation expression} for {@code $toObjectId} that converts a value to a objectId. Shorthand
		 * for {@link #convertTo(String) #convertTo("objectId")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToInt}.
		 */
		public ToObjectId convertToObjectId() {
			return ToObjectId.toObjectId(valueObject());
		}

		/**
		 * Creates new {@link ToInt aggregation expression} for {@code $toString} that converts a value to a string. Shorthand
		 * for {@link #convertTo(String) #convertTo("string")}. <br />
		 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
		 *
		 * @return new instance of {@link ToInt}.
		 */
		public ToString convertToString() {
			return ToString.toString(valueObject());
		}

		/**
		 * {@link AggregationExpression} for {@code $degreesToRadians} that converts an input value measured in degrees to
		 * radians.
		 *
		 * @return new instance of {@link DegreesToRadians}.
		 * @since 3.3
		 */
		public DegreesToRadians convertDegreesToRadians() {
			return DegreesToRadians.degreesToRadians(valueObject());
		}

		private Convert createConvert() {
			return usesFieldRef() ? Convert.convertValueOf(fieldReference) : Convert.convertValueOf(expression);
		}

		private Object valueObject() {
			return usesFieldRef() ? Fields.field(fieldReference) : expression;
		}

		private boolean usesFieldRef() {
			return fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $convert} that converts a value to a specified type. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 * "https://docs.mongodb.com/manual/reference/operator/aggregation/convert/">https://docs.mongodb.com/manual/reference/operator/aggregation/convert/</a>
	 * @since 2.1
	 */
	public static class Convert extends AbstractAggregationExpression {

		private Convert(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link Convert} using the given value for the {@literal input} attribute.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValue(Object value) {
			return new Convert(Collections.singletonMap("input", value));
		}

		/**
		 * Creates new {@link Convert} using the value of the provided {@link Field fieldReference} as {@literal input}
		 * value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValueOf(String fieldReference) {
			return convertValue(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Convert} using the result of the provided {@link AggregationExpression expression} as
		 * {@literal input} value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public static Convert convertValueOf(AggregationExpression expression) {
			return convertValue(expression);
		}

		/**
		 * Specify the conversion target type via its {@link String} representation.
		 * <ul>
		 * <li>double</li>
		 * <li>string</li>
		 * <li>objectId</li>
		 * <li>bool</li>
		 * <li>date</li>
		 * <li>int</li>
		 * <li>long</li>
		 * <li>decimal</li>
		 * </ul>
		 *
		 * @param stringTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(String stringTypeIdentifier) {
			return new Convert(append("to", stringTypeIdentifier));
		}

		/**
		 * Specify the conversion target type via its numeric representation.
		 * <dl>
		 * <dt>1</dt>
		 * <dd>double</dd>
		 * <dt>2</dt>
		 * <dd>string</dd>
		 * <dt>7</dt>
		 * <dd>objectId</dd>
		 * <dt>8</dt>
		 * <dd>bool</dd>
		 * <dt>9</dt>
		 * <dd>date</dd>
		 * <dt>16</dt>
		 * <dd>int</dd>
		 * <dt>18</dt>
		 * <dd>long</dd>
		 * <dt>19</dt>
		 * <dd>decimal</dd>
		 * </dl>
		 *
		 * @param numericTypeIdentifier must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(int numericTypeIdentifier) {
			return new Convert(append("to", numericTypeIdentifier));
		}

		/**
		 * Specify the conversion target type.
		 *
		 * @param type must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert to(Type type) {

			String typeString = Type.BOOLEAN.equals(type) ? "bool" : type.value().toString();
			return to(typeString);
		}

		/**
		 * Specify the conversion target type via the value of the given field.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert toTypeOf(String fieldReference) {
			return new Convert(append("to", Fields.field(fieldReference)));
		}

		/**
		 * Specify the conversion target type via the value of the given {@link AggregationExpression expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert toTypeOf(AggregationExpression expression) {
			return new Convert(append("to", expression));
		}

		/**
		 * Optionally specify the value to return on encountering an error during conversion.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturn(Object value) {
			return new Convert(append("onError", value));
		}

		/**
		 * Optionally specify the field holding the value to return on encountering an error during conversion.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturnValueOf(String fieldReference) {
			return onErrorReturn(Fields.field(fieldReference));
		}

		/**
		 * Optionally specify the expression to evaluate and return on encountering an error during conversion.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onErrorReturnValueOf(AggregationExpression expression) {
			return onErrorReturn(expression);
		}

		/**
		 * Optionally specify the value to return when the input is {@literal null} or missing.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturn(Object value) {
			return new Convert(append("onNull", value));
		}

		/**
		 * Optionally specify the field holding the value to return when the input is {@literal null} or missing.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturnValueOf(String fieldReference) {
			return onNullReturn(Fields.field(fieldReference));
		}

		/**
		 * Optionally specify the expression to evaluate and return when the input is {@literal null} or missing.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Convert}.
		 */
		public Convert onNullReturnValueOf(AggregationExpression expression) {
			return onNullReturn(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$convert";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toBool} that converts a value to {@literal boolean}. Shorthand for
	 * {@link Convert#to(String) Convert#to("bool")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toBool/">https://docs.mongodb.com/manual/reference/operator/aggregation/toBool/</a>
	 * @since 2.1
	 */
	public static class ToBool extends AbstractAggregationExpression {

		private ToBool(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToBool} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToBool}.
		 */
		public static ToBool toBoolean(Object value) {
			return new ToBool(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toBool";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toDate} that converts a value to {@literal date}. Shorthand for
	 * {@link Convert#to(String) Convert#to("date")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toDate/">https://docs.mongodb.com/manual/reference/operator/aggregation/toDate/</a>
	 * @since 2.1
	 */
	public static class ToDate extends AbstractAggregationExpression {

		private ToDate(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToDate} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToDate}.
		 */
		public static ToDate toDate(Object value) {
			return new ToDate(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toDate";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toDecimal} that converts a value to {@literal decimal}. Shorthand for
	 * {@link Convert#to(String) Convert#to("decimal")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toDecimal/">https://docs.mongodb.com/manual/reference/operator/aggregation/toDecimal/</a>
	 * @since 2.1
	 */
	public static class ToDecimal extends AbstractAggregationExpression {

		private ToDecimal(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToDecimal} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToDecimal}.
		 */
		public static ToDecimal toDecimal(Object value) {
			return new ToDecimal(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toDecimal";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toDouble} that converts a value to {@literal double}. Shorthand for
	 * {@link Convert#to(String) Convert#to("double")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toDouble/">https://docs.mongodb.com/manual/reference/operator/aggregation/toDouble/</a>
	 * @since 2.1
	 */
	public static class ToDouble extends AbstractAggregationExpression {

		private ToDouble(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToDouble} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToDouble}.
		 */
		public static ToDouble toDouble(Object value) {
			return new ToDouble(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toDouble";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toInt} that converts a value to {@literal integer}. Shorthand for
	 * {@link Convert#to(String) Convert#to("int")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toInt/">https://docs.mongodb.com/manual/reference/operator/aggregation/toInt/</a>
	 * @since 2.1
	 */
	public static class ToInt extends AbstractAggregationExpression {

		private ToInt(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToInt} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToInt}.
		 */
		public static ToInt toInt(Object value) {
			return new ToInt(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toInt";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toLong} that converts a value to {@literal long}. Shorthand for
	 * {@link Convert#to(String) Convert#to("long")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toLong/">https://docs.mongodb.com/manual/reference/operator/aggregation/toLong/</a>
	 * @since 2.1
	 */
	public static class ToLong extends AbstractAggregationExpression {

		private ToLong(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToLong} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToLong}.
		 */
		public static ToLong toLong(Object value) {
			return new ToLong(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toLong";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toObjectId} that converts a value to {@literal objectId}. Shorthand for
	 * {@link Convert#to(String) Convert#to("objectId")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toObjectId/">https://docs.mongodb.com/manual/reference/operator/aggregation/toObjectId/</a>
	 * @since 2.1
	 */
	public static class ToObjectId extends AbstractAggregationExpression {

		private ToObjectId(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToObjectId} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToObjectId}.
		 */
		public static ToObjectId toObjectId(Object value) {
			return new ToObjectId(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toObjectId";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $toString} that converts a value to {@literal string}. Shorthand for
	 * {@link Convert#to(String) Convert#to("string")}. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 4.0 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/toString/">https://docs.mongodb.com/manual/reference/operator/aggregation/toString/</a>
	 * @since 2.1
	 */
	public static class ToString extends AbstractAggregationExpression {

		private ToString(Object value) {
			super(value);
		}

		/**
		 * Creates new {@link ToString} using the given value as input.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link ToString}.
		 */
		public static ToString toString(Object value) {
			return new ToString(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$toString";
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $degreesToRadians} that converts an input value measured in degrees to radians.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 */
	public static class DegreesToRadians extends AbstractAggregationExpression {

		private DegreesToRadians(Object value) {
			super(value);
		}

		/**
		 * Create a new instance of {@link DegreesToRadians} that converts the value of the given field, measured in degrees, to radians.
		 *
		 * @param fieldName must not be {@literal null}.
		 * @return new instance of {@link DegreesToRadians}.
		 */
		public static DegreesToRadians degreesToRadiansOf(String fieldName) {
			return degreesToRadians(Fields.field(fieldName));
		}

		/**
		 * Create a new instance of {@link DegreesToRadians} that converts the result of the given {@link AggregationExpression expression}, measured in degrees, to radians.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link DegreesToRadians}.
		 */
		public static DegreesToRadians degreesToRadiansOf(AggregationExpression expression) {
			return degreesToRadians(expression);
		}

		/**
		 * Create a new instance of {@link DegreesToRadians} that converts the given value, measured in degrees, to radians.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link DegreesToRadians}.
		 */
		public static DegreesToRadians degreesToRadians(Object value) {
			return new DegreesToRadians(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$degreesToRadians";
		}
	}
}
