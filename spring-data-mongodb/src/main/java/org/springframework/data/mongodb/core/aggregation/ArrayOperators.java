/*
 * Copyright 2016-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.bson.Document;
import org.springframework.data.domain.Range;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Filter.AsBuilder;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators.Reduce.PropertyExpression;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal array} aggregation operations.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Shashank Sharma
 * @since 1.0
 */
public class ArrayOperators {

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link ArrayOperatorFactory}.
	 */
	public static ArrayOperatorFactory arrayOf(String fieldReference) {
		return new ArrayOperatorFactory(fieldReference);
	}

	/**
	 * Take the array referenced resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link ArrayOperatorFactory}.
	 */
	public static ArrayOperatorFactory arrayOf(AggregationExpression expression) {
		return new ArrayOperatorFactory(expression);
	}

	/**
	 * Take the given {@link Collection values} {@link AggregationExpression}.
	 *
	 * @param values must not be {@literal null}.
	 * @return new instance of {@link ArrayOperatorFactory}.
	 * @since 2.2
	 */
	public static ArrayOperatorFactory arrayOf(Collection<?> values) {
		return new ArrayOperatorFactory(values);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class ArrayOperatorFactory {

		private final @Nullable String fieldReference;
		private final @Nullable AggregationExpression expression;
		private final @Nullable Collection values;

		/**
		 * Creates new {@link ArrayOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public ArrayOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
			this.values = null;
		}

		/**
		 * Creates new {@link ArrayOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ArrayOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
			this.values = null;
		}

		/**
		 * Creates new {@link ArrayOperatorFactory} for given values.
		 *
		 * @param values must not be {@literal null}.
		 * @since 2.2
		 */
		public ArrayOperatorFactory(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			this.fieldReference = null;
			this.expression = null;
			this.values = values;
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and returns the element at the
		 * specified array {@literal position}.
		 *
		 * @param position the element index.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(int position) {
			return createArrayElemAt().elementAt(position);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and returns the element at the position
		 * resulting form the given {@literal expression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createArrayElemAt().elementAt(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and returns the element at the position
		 * defined by the referenced {@literal field}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return createArrayElemAt().elementAt(fieldReference);
		}

		private ArrayElemAt createArrayElemAt() {

			if (usesFieldRef()) {
				return ArrayElemAt.arrayOf(fieldReference);
			}

			return usesExpression() ? ArrayElemAt.arrayOf(expression) : ArrayElemAt.arrayOf(values);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and concats the given
		 * {@literal arrayFieldReference} to it.
		 *
		 * @param arrayFieldReference must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public ConcatArrays concat(String arrayFieldReference) {

			Assert.notNull(arrayFieldReference, "ArrayFieldReference must not be null!");
			return createConcatArrays().concat(arrayFieldReference);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and concats the array resulting form
		 * the given {@literal expression} to it.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public ConcatArrays concat(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return createConcatArrays().concat(expression);
		}

		private ConcatArrays createConcatArrays() {

			if (usesFieldRef()) {
				return ConcatArrays.arrayOf(fieldReference);
			}

			return usesExpression() ? ConcatArrays.arrayOf(expression) : ConcatArrays.arrayOf(values);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and selects a subset of the array to
		 * return based on the specified condition.
		 *
		 * @return new instance of {@link AsBuilder} to create a {@link Filter}.
		 */
		public AsBuilder filter() {

			if (usesFieldRef()) {
				return Filter.filter(fieldReference);
			}

			Assert.state(values != null, "Values must not be null!");
			return Filter.filter(new ArrayList<>(values));
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and an check if its an array.
		 *
		 * @return new instance of {@link IsArray}.
		 */
		public IsArray isArray() {

			Assert.state(values == null, "Does it make sense to call isArray on an array? Maybe just skip it?");

			return usesFieldRef() ? IsArray.isArray(fieldReference) : IsArray.isArray(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and retrieves its length.
		 *
		 * @return new instance of {@link Size}.
		 */
		public Size length() {

			if (usesFieldRef()) {
				return Size.lengthOfArray(fieldReference);
			}

			return usesExpression() ? Size.lengthOfArray(expression) : Size.lengthOfArray(values);
		}

		/**
		 * Creates new {@link AggregationExpression} that takes the associated array and selects a subset from it.
		 *
		 * @return new instance of {@link Slice}.
		 */
		public Slice slice() {

			if (usesFieldRef()) {
				return Slice.sliceArrayOf(fieldReference);
			}

			return usesExpression() ? Slice.sliceArrayOf(expression) : Slice.sliceArrayOf(values);
		}

		/**
		 * Creates new {@link AggregationExpression} that searches the associated array for an occurrence of a specified
		 * value and returns the array index (zero-based) of the first occurrence.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link IndexOfArray}.
		 */
		public IndexOfArray indexOf(Object value) {

			if (usesFieldRef()) {
				return IndexOfArray.arrayOf(fieldReference).indexOf(value);
			}

			return usesExpression() ? IndexOfArray.arrayOf(expression).indexOf(value)
					: IndexOfArray.arrayOf(values).indexOf(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns an array with the elements in reverse order.
		 *
		 * @return new instance of {@link ReverseArray}.
		 */
		public ReverseArray reverse() {

			if (usesFieldRef()) {
				return ReverseArray.reverseArrayOf(fieldReference);
			}

			return usesExpression() ? ReverseArray.reverseArrayOf(expression)
					: ReverseArray.reverseArrayOf(Collections.singletonList(values));
		}

		/**
		 * Start creating new {@link AggregationExpression} that applies an {@link AggregationExpression} to each element in
		 * an array and combines them into a single value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReduceInitialValueBuilder} to create {@link Reduce}.
		 */
		public ArrayOperatorFactory.ReduceInitialValueBuilder reduce(AggregationExpression expression) {

			return initialValue -> (usesFieldRef() ? Reduce.arrayOf(fieldReference)
					: Reduce.arrayOf(ArrayOperatorFactory.this.expression)).withInitialValue(initialValue).reduce(expression);
		}

		/**
		 * Start creating new {@link AggregationExpression} that applies an {@link AggregationExpression} to each element in
		 * an array and combines them into a single value.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return new instance of {@link ReduceInitialValueBuilder} to create {@link Reduce}.
		 */
		public ArrayOperatorFactory.ReduceInitialValueBuilder reduce(PropertyExpression... expressions) {

			return initialValue -> (usesFieldRef() ? Reduce.arrayOf(fieldReference) : Reduce.arrayOf(expression))
					.withInitialValue(initialValue).reduce(expressions);
		}

		/**
		 * Creates new {@link AggregationExpression} that transposes an array of input arrays so that the first element of
		 * the output array would be an array containing, the first element of the first input array, the first element of
		 * the second input array, etc.
		 *
		 * @param arrays must not be {@literal null}.
		 * @return new instance of {@link Zip}.
		 */
		public Zip zipWith(Object... arrays) {

			if (usesFieldRef()) {
				return Zip.arrayOf(fieldReference).zip(arrays);
			}

			return (usesExpression() ? Zip.arrayOf(expression) : Zip.arrayOf(values)).zip(arrays);
		}

		/**
		 * Creates new {@link AggregationExpression} that returns a boolean indicating whether a specified value is in the
		 * associated array.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link In}.
		 */
		public In containsValue(Object value) {

			if (usesFieldRef()) {
				return In.arrayOf(fieldReference).containsValue(value);
			}

			return (usesExpression() ? In.arrayOf(expression) : In.arrayOf(values)).containsValue(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that converts the associated expression into an object.
		 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
		 *
		 * @return new instance of {@link ArrayToObject}.
		 * @since 2.1
		 */
		public ArrayToObject toObject() {

			if (usesFieldRef()) {
				return ArrayToObject.arrayValueOfToObject(fieldReference);
			}

			return usesExpression() ? ArrayToObject.arrayValueOfToObject(expression) : ArrayToObject.arrayToObject(values);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ReduceInitialValueBuilder {

			/**
			 * Define the initial cumulative value set before in is applied to the first element of the input array.
			 *
			 * @param initialValue must not be {@literal null}.
			 * @return
			 */
			Reduce startingWith(Object initialValue);
		}

		/**
		 * @return {@literal true} if {@link #fieldReference} is not {@literal null}.
		 */
		private boolean usesFieldRef() {
			return fieldReference != null;
		}

		/**
		 * @return {@literal true} if {@link #expression} is not {@literal null}.
		 * @since 2.2
		 */
		private boolean usesExpression() {
			return expression != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $arrayElementAt}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ArrayElemAt extends AbstractAggregationExpression {

		private ArrayElemAt(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$arrayElemAt";
		}

		/**
		 * Creates new {@link ArrayElemAt}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public static ArrayElemAt arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ArrayElemAt(asFields(fieldReference));
		}

		/**
		 * Creates new {@link ArrayElemAt}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public static ArrayElemAt arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ArrayElemAt(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ArrayElemAt}.
		 *
		 * @param values The array members. Must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 * @since 2.2
		 */
		public static ArrayElemAt arrayOf(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new ArrayElemAt(Collections.singletonList(values));
		}

		/**
		 * Use the element with given index number.
		 *
		 * @param index the index number
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(int index) {
			return new ArrayElemAt(append(index));
		}

		/**
		 * Use the element at the index number evaluated from the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ArrayElemAt(append(expression));
		}

		/**
		 * Use the element at the index number traken from the given field.
		 *
		 * @param arrayFieldReference the field name.
		 * @return new instance of {@link ArrayElemAt}.
		 */
		public ArrayElemAt elementAt(String arrayFieldReference) {

			Assert.notNull(arrayFieldReference, "ArrayReference must not be null!");
			return new ArrayElemAt(append(Fields.field(arrayFieldReference)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $concatArrays}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ConcatArrays extends AbstractAggregationExpression {

		private ConcatArrays(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$concatArrays";
		}

		/**
		 * Creates new {@link ConcatArrays}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public static ConcatArrays arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ConcatArrays(asFields(fieldReference));
		}

		/**
		 * Creates new {@link ConcatArrays}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public static ConcatArrays arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ConcatArrays(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link ConcatArrays}.
		 *
		 * @param values The array members. Must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 * @since 2.2
		 */
		public static ConcatArrays arrayOf(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new ConcatArrays(Collections.singletonList(values));
		}

		/**
		 * Concat with the array stored at the given field.
		 *
		 * @param arrayFieldReference must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public ConcatArrays concat(String arrayFieldReference) {

			Assert.notNull(arrayFieldReference, "ArrayFieldReference must not be null!");
			return new ConcatArrays(append(Fields.field(arrayFieldReference)));
		}

		/**
		 * Concat with the array resulting from the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ConcatArrays}.
		 */
		public ConcatArrays concat(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ConcatArrays(append(expression));
		}
	}

	/**
	 * {@code $filter} {@link AggregationExpression} allows to select a subset of the array to return based on the
	 * specified condition.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	public static class Filter implements AggregationExpression {

		private @Nullable Object input;
		private @Nullable ExposedField as;
		private @Nullable Object condition;

		private Filter() {
			// used by builder
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(String field) {

			Assert.notNull(field, "Field must not be null!");
			return filter(Fields.field(field));
		}

		/**
		 * Set the {@literal field} to apply the {@code $filter} to.
		 *
		 * @param field must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static AsBuilder filter(Field field) {

			Assert.notNull(field, "Field must not be null!");
			return new FilterExpressionBuilder().filter(field);
		}

		/**
		 * Set the {@literal values} to apply the {@code $filter} to.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link AsBuilder} to create the {@link Filter}.
		 */
		public static AsBuilder filter(List<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new FilterExpressionBuilder().filter(values);
		}

		@Override
		public Document toDocument(final AggregationOperationContext context) {
			return toFilter(ExposedFields.from(as), context);
		}

		private Document toFilter(ExposedFields exposedFields, AggregationOperationContext context) {

			Document filterExpression = new Document();
			InheritingExposedFieldsAggregationOperationContext operationContext = new InheritingExposedFieldsAggregationOperationContext(
					exposedFields, context);

			filterExpression.putAll(context.getMappedObject(new Document("input", getMappedInput(context))));
			filterExpression.put("as", as.getTarget());

			filterExpression.putAll(context.getMappedObject(new Document("cond", getMappedCondition(operationContext))));

			return new Document("$filter", filterExpression);
		}

		private Object getMappedInput(AggregationOperationContext context) {
			return input instanceof Field ? context.getReference((Field) input).toString() : input;
		}

		private Object getMappedCondition(AggregationOperationContext context) {

			if (!(condition instanceof AggregationExpression)) {
				return condition;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context, Collections.singleton(as));
			return ((AggregationExpression) condition).toDocument(nea);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InputBuilder {

			/**
			 * Set the {@literal values} to apply the {@code $filter} to.
			 *
			 * @param array must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(List<?> array);

			/**
			 * Set the {@literal field} holding an array to apply the {@code $filter} to.
			 *
			 * @param field must not be {@literal null}.
			 * @return
			 */
			AsBuilder filter(Field field);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface AsBuilder {

			/**
			 * Set the {@literal variableName} for the elements in the input array.
			 *
			 * @param variableName must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			ConditionBuilder as(String variableName);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ConditionBuilder {

			/**
			 * Set the {@link AggregationExpression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			Filter by(AggregationExpression expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			Filter by(String expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			Filter by(Document expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static final class FilterExpressionBuilder implements InputBuilder, AsBuilder, ConditionBuilder {

			private final Filter filter;

			FilterExpressionBuilder() {
				this.filter = new Filter();
			}

			/**
			 * Creates new {@link InputBuilder}.
			 *
			 * @return new instance of {@link FilterExpressionBuilder}.
			 */
			public static InputBuilder newBuilder() {
				return new FilterExpressionBuilder();
			}

			@Override
			public AsBuilder filter(List<?> array) {

				Assert.notNull(array, "Array must not be null!");
				filter.input = new ArrayList<Object>(array);
				return this;
			}

			@Override
			public AsBuilder filter(Field field) {

				Assert.notNull(field, "Field must not be null!");
				filter.input = field;
				return this;
			}

			@Override
			public ConditionBuilder as(String variableName) {

				Assert.notNull(variableName, "Variable name  must not be null!");
				filter.as = new ExposedField(variableName, true);
				return this;
			}

			@Override
			public Filter by(AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null!");
				filter.condition = condition;
				return filter;
			}

			@Override
			public Filter by(String expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}

			@Override
			public Filter by(Document expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $isArray}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IsArray extends AbstractAggregationExpression {

		private IsArray(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$isArray";
		}

		/**
		 * Creates new {@link IsArray}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IsArray}.
		 */
		public static IsArray isArray(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IsArray(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link IsArray}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IsArray}.
		 */
		public static IsArray isArray(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IsArray(expression);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $size}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Size extends AbstractAggregationExpression {

		private Size(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$size";
		}

		/**
		 * Creates new {@link Size}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Size}.
		 */
		public static Size lengthOfArray(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Size(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link Size}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Size}.
		 */
		public static Size lengthOfArray(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Size(expression);
		}

		/**
		 * Creates new {@link Size}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link Size}.
		 * @since 2.2
		 */
		public static Size lengthOfArray(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new Size(Collections.singletonList(values));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $slice}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Slice extends AbstractAggregationExpression {

		private Slice(List<?> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$slice";
		}

		/**
		 * Creates new {@link Slice}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Slice}.
		 */
		public static Slice sliceArrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Slice(asFields(fieldReference));
		}

		/**
		 * Creates new {@link Slice}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Slice}.
		 */
		public static Slice sliceArrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Slice(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link Slice}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link Slice}.
		 * @since 2.2
		 */
		public static Slice sliceArrayOf(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new Slice(Collections.singletonList(values));
		}

		/**
		 * Slice the number of elements.
		 *
		 * @param nrElements elements to slice.
		 * @return new instance of {@link Slice}.
		 */
		public Slice itemCount(int nrElements) {
			return new Slice(append(nrElements));
		}

		/**
		 * Slice using offset and count.
		 *
		 * @param position the start position
		 * @return new instance of {@link SliceElementsBuilder} to create {@link Slice}.
		 */
		public SliceElementsBuilder offset(final int position) {

			return new SliceElementsBuilder() {

				@Override
				public Slice itemCount(int nrElements) {
					return new Slice(append(position)).itemCount(nrElements);
				}
			};
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface SliceElementsBuilder {

			/**
			 * Set the number of elements given {@literal nrElements}.
			 *
			 * @param nrElements
			 * @return
			 */
			Slice itemCount(int nrElements);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $indexOfArray}.
	 *
	 * @author Christoph Strobl
	 */
	public static class IndexOfArray extends AbstractAggregationExpression {

		private IndexOfArray(List<Object> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$indexOfArray";
		}

		/**
		 * Start creating new {@link IndexOfArray}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link IndexOfArray}.
		 */
		public static IndexOfArrayBuilder arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new IndexOfArrayBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating new {@link IndexOfArray}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link IndexOfArray}.
		 */
		public static IndexOfArrayBuilder arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new IndexOfArrayBuilder(expression);
		}

		/**
		 * Start creating new {@link IndexOfArray}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link IndexOfArrayBuilder} to create {@link IndexOfArray}.
		 * @since 2.2
		 */
		public static IndexOfArrayBuilder arrayOf(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new IndexOfArrayBuilder(values);
		}

		/**
		 * Lookup within a given range.
		 *
		 * @param range the lookup range.
		 * @return new instance of {@link IndexOfArray}.
		 */
		public IndexOfArray within(Range<Long> range) {
			return new IndexOfArray(append(AggregationUtils.toRangeValues(range)));
		}

		/**
		 * @author Christoph Strobl
		 */
		public static class IndexOfArrayBuilder {

			private final Object targetArray;

			private IndexOfArrayBuilder(Object targetArray) {
				this.targetArray = targetArray;
			}

			/**
			 * Set the {@literal value} to check for its index in the array.
			 *
			 * @param value must not be {@literal null}.
			 * @return new instance of {@link IndexOfArray}.
			 */
			public IndexOfArray indexOf(Object value) {

				Assert.notNull(value, "Value must not be null!");
				return new IndexOfArray(Arrays.asList(targetArray, value));
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $range}.
	 *
	 * @author Christoph Strobl
	 */
	public static class RangeOperator extends AbstractAggregationExpression {

		private RangeOperator(List<Object> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
			return "$range";
		}

		/**
		 * Start creating new {@link RangeOperator}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link RangeOperatorBuilder} to create {@link RangeOperator}.
		 */
		public static RangeOperatorBuilder rangeStartingAt(String fieldReference) {
			return new RangeOperatorBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating new {@link RangeOperator}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link RangeOperatorBuilder} to create {@link RangeOperator}.
		 */
		public static RangeOperatorBuilder rangeStartingAt(AggregationExpression expression) {
			return new RangeOperatorBuilder(expression);
		}

		/**
		 * Start creating new {@link RangeOperator}.
		 *
		 * @param value
		 * @return new instance of {@link RangeOperator}.
		 */
		public static RangeOperatorBuilder rangeStartingAt(long value) {
			return new RangeOperatorBuilder(value);
		}

		public RangeOperator withStepSize(long stepSize) {
			return new RangeOperator(append(stepSize));
		}

		public static class RangeOperatorBuilder {

			private final Object startPoint;

			private RangeOperatorBuilder(Object startPoint) {
				this.startPoint = startPoint;
			}

			/**
			 * Creates new {@link RangeOperator}.
			 *
			 * @param index
			 * @return new instance of {@link RangeOperator}.
			 */
			public RangeOperator to(long index) {
				return new RangeOperator(Arrays.asList(startPoint, index));
			}

			/**
			 * Creates new {@link RangeOperator}.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link RangeOperator}.
			 */
			public RangeOperator to(AggregationExpression expression) {
				return new RangeOperator(Arrays.asList(startPoint, expression));
			}

			/**
			 * Creates new {@link RangeOperator}.
			 *
			 * @param fieldReference must not be {@literal null}.
			 * @return new instance of {@link RangeOperator}.
			 */
			public RangeOperator to(String fieldReference) {
				return new RangeOperator(Arrays.asList(startPoint, Fields.field(fieldReference)));
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $reverseArray}.
	 *
	 * @author Christoph Strobl
	 */
	public static class ReverseArray extends AbstractAggregationExpression {

		private ReverseArray(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$reverseArray";
		}

		/**
		 * Creates new {@link ReverseArray} given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ReverseArray}.
		 */
		public static ReverseArray reverseArrayOf(String fieldReference) {
			return new ReverseArray(Fields.field(fieldReference));
		}

		/**
		 * Creates new {@link ReverseArray} given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ReverseArray}.
		 */
		public static ReverseArray reverseArrayOf(AggregationExpression expression) {
			return new ReverseArray(expression);
		}

		/**
		 * Creates new {@link ReverseArray}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link ReverseArray}.
		 * @since 2.2
		 */
		public static ReverseArray reverseArrayOf(Collection<?> values) {
			return new ReverseArray(values);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $reduce}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Reduce implements AggregationExpression {

		private final Object input;
		private final Object initialValue;
		private final List<AggregationExpression> reduceExpressions;

		private Reduce(Object input, Object initialValue, List<AggregationExpression> reduceExpressions) {

			this.input = input;
			this.initialValue = initialValue;
			this.reduceExpressions = reduceExpressions;
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {

			Document document = new Document();

			document.put("input", getMappedValue(input, context));
			document.put("initialValue", getMappedValue(initialValue, context));

			if (reduceExpressions.iterator().next() instanceof PropertyExpression) {

				Document properties = new Document();
				for (AggregationExpression e : reduceExpressions) {
					properties.putAll(e.toDocument(context));
				}
				document.put("in", properties);
			} else {
				document.put("in", (reduceExpressions.iterator().next()).toDocument(context));
			}

			return new Document("$reduce", document);
		}

		private Object getMappedValue(Object value, AggregationOperationContext context) {

			if (value instanceof Document) {
				return value;
			}
			if (value instanceof AggregationExpression) {
				return ((AggregationExpression) value).toDocument(context);
			} else if (value instanceof Field) {
				return context.getReference(((Field) value)).toString();
			} else {
				return context.getMappedObject(new Document("###val###", value)).get("###val###");
			}
		}

		/**
		 * Start creating new {@link Reduce}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link InitialValueBuilder} to create {@link Reduce}.
		 */
		public static InitialValueBuilder arrayOf(final String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			return new InitialValueBuilder() {

				@Override
				public ReduceBuilder withInitialValue(final Object initialValue) {

					Assert.notNull(initialValue, "Initial value must not be null");

					return new ReduceBuilder() {

						@Override
						public Reduce reduce(AggregationExpression expression) {

							Assert.notNull(expression, "AggregationExpression must not be null");
							return new Reduce(Fields.field(fieldReference), initialValue, Collections.singletonList(expression));
						}

						@Override
						public Reduce reduce(PropertyExpression... expressions) {

							Assert.notNull(expressions, "PropertyExpressions must not be null");

							return new Reduce(Fields.field(fieldReference), initialValue,
									Arrays.<AggregationExpression>asList(expressions));
						}
					};
				}
			};
		}

		/**
		 * Start creating new {@link Reduce}.
		 *
		 * @param arrayValueExpression must not be {@literal null}.
		 * @return new instance of {@link InitialValueBuilder} to create {@link Reduce}.
		 */
		public static InitialValueBuilder arrayOf(final AggregationExpression arrayValueExpression) {

			return new InitialValueBuilder() {

				@Override
				public ReduceBuilder withInitialValue(final Object initialValue) {

					Assert.notNull(initialValue, "Initial value must not be null");

					return new ReduceBuilder() {

						@Override
						public Reduce reduce(AggregationExpression expression) {

							Assert.notNull(expression, "AggregationExpression must not be null");
							return new Reduce(arrayValueExpression, initialValue, Collections.singletonList(expression));
						}

						@Override
						public Reduce reduce(PropertyExpression... expressions) {

							Assert.notNull(expressions, "PropertyExpressions must not be null");
							return new Reduce(arrayValueExpression, initialValue, Arrays.asList(expressions));
						}
					};
				}
			};
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InitialValueBuilder {

			/**
			 * Define the initial cumulative value set before in is applied to the first element of the input array.
			 *
			 * @param initialValue must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			ReduceBuilder withInitialValue(Object initialValue);
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface ReduceBuilder {

			/**
			 * Define the {@link AggregationExpression} to apply to each element in the input array in left-to-right order.
			 * <br />
			 * <b>NOTE:</b> During evaluation of the in expression the variable references {@link Variable#THIS} and
			 * {@link Variable#VALUE} are available.
			 *
			 * @param expression must not be {@literal null}.
			 * @return new instance of {@link Reduce}.
			 */
			Reduce reduce(AggregationExpression expression);

			/**
			 * Define the {@link PropertyExpression}s to apply to each element in the input array in left-to-right order.
			 * <br />
			 * <b>NOTE:</b> During evaluation of the in expression the variable references {@link Variable#THIS} and
			 * {@link Variable#VALUE} are available.
			 *
			 * @param expressions must not be {@literal null}.
			 * @return new instance of {@link Reduce}.
			 */
			Reduce reduce(PropertyExpression... expressions);
		}

		/**
		 * @author Christoph Strobl
		 */
		public static class PropertyExpression implements AggregationExpression {

			private final String propertyName;
			private final AggregationExpression aggregationExpression;

			protected PropertyExpression(String propertyName, AggregationExpression aggregationExpression) {

				Assert.notNull(propertyName, "Property name must not be null!");
				Assert.notNull(aggregationExpression, "AggregationExpression must not be null!");

				this.propertyName = propertyName;
				this.aggregationExpression = aggregationExpression;
			}

			/**
			 * Define a result property for an {@link AggregationExpression} used in {@link Reduce}.
			 *
			 * @param name must not be {@literal null}.
			 * @return new instance of {@link AsBuilder} to create {@link Reduce}.
			 */
			public static AsBuilder property(final String name) {

				return new AsBuilder() {

					@Override
					public PropertyExpression definedAs(AggregationExpression expression) {
						return new PropertyExpression(name, expression);
					}
				};
			}

			@Override
			public Document toDocument(AggregationOperationContext context) {
				return new Document(propertyName, aggregationExpression.toDocument(context));
			}

			/**
			 * @author Christoph Strobl
			 */
			public interface AsBuilder {

				/**
				 * Set the {@link AggregationExpression} resulting in the properties value.
				 *
				 * @param expression must not be {@literal null}.
				 * @return never {@literal null}.
				 */
				PropertyExpression definedAs(AggregationExpression expression);
			}
		}

		public enum Variable implements Field {

			THIS {
				@Override
				public String getName() {
					return "$$this";
				}

				@Override
				public String getTarget() {
					return "$$this";
				}

				@Override
				public boolean isAliased() {
					return false;
				}

				@Override
				public String toString() {
					return getName();
				}
			},

			VALUE {
				@Override
				public String getName() {
					return "$$value";
				}

				@Override
				public String getTarget() {
					return "$$value";
				}

				@Override
				public boolean isAliased() {
					return false;
				}

				@Override
				public String toString() {
					return getName();
				}
			};

			/**
			 * Create a {@link Field} reference to a given {@literal property} prefixed with the {@link Variable} identifier.
			 * eg. {@code $$value.product}
			 *
			 * @param property must not be {@literal null}.
			 * @return never {@literal null}.
			 */
			public Field referringTo(final String property) {

				return new Field() {
					@Override
					public String getName() {
						return Variable.this.getName() + "." + property;
					}

					@Override
					public String getTarget() {
						return Variable.this.getTarget() + "." + property;
					}

					@Override
					public boolean isAliased() {
						return false;
					}

					@Override
					public String toString() {
						return getName();
					}
				};
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $zip}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Zip extends AbstractAggregationExpression {

		protected Zip(java.util.Map<String, Object> value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$zip";
		}

		/**
		 * Start creating new {@link Zip}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ZipBuilder} to create {@link Zip}.
		 */
		public static ZipBuilder arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new ZipBuilder(Fields.field(fieldReference));
		}

		/**
		 * Start creating new {@link Zip}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ZipBuilder} to create {@link Zip}.
		 */
		public static ZipBuilder arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new ZipBuilder(expression);
		}

		/**
		 * Start creating new {@link Zip}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link Zip}.
		 * @since 2.2
		 */
		public static ZipBuilder arrayOf(Collection<?> values) {

			Assert.notNull(values, "Expression must not be null!");
			return new ZipBuilder(values);
		}

		/**
		 * Create new {@link Zip} and set the {@code useLongestLength} property to {@literal true}.
		 *
		 * @return new instance of {@link Zip}.
		 */
		public Zip useLongestLength() {
			return new Zip(append("useLongestLength", true));
		}

		/**
		 * Optionally provide a default value.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link Zip}.
		 */
		public Zip defaultTo(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			return new Zip(append("defaults", Fields.field(fieldReference)));
		}

		/**
		 * Optionally provide a default value.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link Zip}.
		 */
		public Zip defaultTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new Zip(append("defaults", expression));
		}

		/**
		 * Optionally provide a default value.
		 *
		 * @param array must not be {@literal null}.
		 * @return new instance of {@link Zip}.
		 */
		public Zip defaultTo(Object[] array) {

			Assert.notNull(array, "Array must not be null!");
			return new Zip(append("defaults", Arrays.asList(array)));
		}

		public static class ZipBuilder {

			private final List<Object> sourceArrays;

			private ZipBuilder(Object sourceArray) {

				this.sourceArrays = new ArrayList<Object>();
				this.sourceArrays.add(sourceArray);
			}

			/**
			 * Creates new {@link Zip} that transposes an array of input arrays so that the first element of the output array
			 * would be an array containing, the first element of the first input array, the first element of the second input
			 * array, etc.
			 *
			 * @param arrays arrays to zip the referenced one with. must not be {@literal null}.
			 * @return new instance of {@link Zip}.
			 */
			public Zip zip(Object... arrays) {

				Assert.notNull(arrays, "Arrays must not be null!");
				for (Object value : arrays) {

					if (value instanceof String) {
						sourceArrays.add(Fields.field((String) value));
					} else {
						sourceArrays.add(value);
					}
				}

				return new Zip(Collections.<String, Object>singletonMap("inputs", sourceArrays));
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $in}.
	 *
	 * @author Christoph Strobl
	 * @author Shashank Sharma
	 * @see <a href=
	 * "https://docs.mongodb.com/manual/reference/operator/aggregation/in/">https://docs.mongodb.com/manual/reference/operator/aggregation/in/</a>
	 * @since 2.2
	 */
	public static class In extends AbstractAggregationExpression {

		private In(List<Object> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
			return "$in";
		}

		/**
		 * Start creating {@link In}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link InBuilder} to create {@link In}.
		 */
		public static InBuilder arrayOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");

			return value -> {

				Assert.notNull(value, "Value must not be null!");
				return new In(Arrays.asList(value, Fields.field(fieldReference)));
			};
		}

		/**
		 * Start creating {@link In}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link InBuilder} to create {@link In}.
		 */
		public static InBuilder arrayOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");

			return value -> {

				Assert.notNull(value, "Value must not be null!");

				return new In(Arrays.asList(value, expression));
			};
		}

		/**
		 * Support for Aggregation In Search an Element in List of Objects to Filter Start creating {@link In}.
		 *
		 * @param values must not be {@literal null}.
		 * @return new instance of {@link InBuilder}.
		 * @since 2.2
		 */
		public static InBuilder arrayOf(Collection<?> values) {

			Assert.notNull(values, "Values must not be null!");

			return value -> {

				Assert.notNull(value, "Value must not be null!");

				return new In(Arrays.asList(value, values));
			};
		}

		/**
		 * @author Christoph Strobl
		 */
		public interface InBuilder {

			/**
			 * Set the {@literal value} to check for existence in the array.
			 *
			 * @param value must not be {@literal value}.
			 * @return new instance of {@link In}.
			 */
			In containsValue(Object value);
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $arrayToObject} that transforms an array into a single document. <br />
	 * <strong>NOTE:</strong> Requires MongoDB 3.6 or later.
	 *
	 * @author Christoph Strobl
	 * @see <a href=
	 * "https://docs.mongodb.com/manual/reference/operator/aggregation/arrayToObject/">https://docs.mongodb.com/manual/reference/operator/aggregation/arrayToObject/</a>
	 * @since 2.1
	 */
	public static class ArrayToObject extends AbstractAggregationExpression {

		private ArrayToObject(Object value) {
			super(value);
		}

		/**
		 * Converts the given array (e.g. an array of two-element arrays, a field reference to an array,...) to an object.
		 *
		 * @param array must not be {@literal null}.
		 * @return new instance of {@link ArrayToObject}.
		 */
		public static ArrayToObject arrayToObject(Object array) {
			return new ArrayToObject(array);
		}

		/**
		 * Converts the array pointed to by the given {@link Field field reference} to an object.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link ArrayToObject}.
		 */
		public static ArrayToObject arrayValueOfToObject(String fieldReference) {
			return new ArrayToObject(Fields.field(fieldReference));
		}

		/**
		 * Converts the result array of the given {@link AggregationExpression expression} to an object.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link ArrayToObject}.
		 */
		public static ArrayToObject arrayValueOfToObject(AggregationExpression expression) {
			return new ArrayToObject(expression);
		}

		@Override
		protected String getMongoMethod() {
			return "$arrayToObject";
		}
	}
}
