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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.AccumulatorOperators.Sum;
import org.springframework.util.Assert;

/**
 * Gateway to {@literal Set expressions} which perform {@literal set} operation on arrays, treating arrays as sets.
 *
 * @author Christoph Strobl
 * @since 1.10
 */
public class SetOperators {

	/**
	 * Take the array referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return
	 */
	public static SetOperatorFactory arrayAsSet(String fieldReference) {
		return new SetOperatorFactory(fieldReference);
	}

	/**
	 * Take the array resulting from the given {@link AggregationExpression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return
	 */
	public static SetOperatorFactory arrayAsSet(AggregationExpression expression) {
		return new SetOperatorFactory(expression);
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class SetOperatorFactory {

		private final String fieldReference;
		private final AggregationExpression expression;

		/**
		 * Creates new {@link SetOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public SetOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null!");
			this.fieldReference = fieldReference;
			this.expression = null;
		}

		/**
		 * Creates new {@link SetOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public SetOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			this.fieldReference = null;
			this.expression = expression;
		}

		/**
		 * Creates new {@link AggregationExpressions} that compares the previously mentioned field to one or more arrays and
		 * returns {@literal true} if they have the same distinct elements and {@literal false} otherwise.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetEquals isEqualTo(String... arrayReferences) {
			return createSetEquals().isEqualTo(arrayReferences);
		}

		/**
		 * Creates new {@link AggregationExpressions} that compares the previously mentioned field to one or more arrays and
		 * returns {@literal true} if they have the same distinct elements and {@literal false} otherwise.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetEquals isEqualTo(AggregationExpression... expressions) {
			return createSetEquals().isEqualTo(expressions);
		}

		private SetEquals createSetEquals() {
			return usesFieldRef() ? SetEquals.arrayAsSet(fieldReference) : SetEquals.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
		 * arrays and returns an array that contains the elements that appear in every of those.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetIntersection intersects(String... arrayReferences) {
			return createSetIntersection().intersects(arrayReferences);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
		 * arrays and returns an array that contains the elements that appear in every of those.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetIntersection intersects(AggregationExpression... expressions) {
			return createSetIntersection().intersects(expressions);
		}

		private SetIntersection createSetIntersection() {
			return usesFieldRef() ? SetIntersection.arrayAsSet(fieldReference) : SetIntersection.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
		 * arrays and returns an array that contains the elements that appear in any of those.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetUnion union(String... arrayReferences) {
			return createSetUnion().union(arrayReferences);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and one or more
		 * arrays and returns an array that contains the elements that appear in any of those.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetUnion union(AggregationExpression... expressions) {
			return createSetUnion().union(expressions);
		}

		private SetUnion createSetUnion() {
			return usesFieldRef() ? SetUnion.arrayAsSet(fieldReference) : SetUnion.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns an
		 * array containing the elements that do not exist in the given {@literal arrayReference}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public SetDifference differenceTo(String arrayReference) {
			return createSetDifference().differenceTo(arrayReference);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns an
		 * array containing the elements that do not exist in the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public SetDifference differenceTo(AggregationExpression expression) {
			return createSetDifference().differenceTo(expression);
		}

		private SetDifference createSetDifference() {
			return usesFieldRef() ? SetDifference.arrayAsSet(fieldReference) : SetDifference.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns
		 * {@literal true} if it is a subset of the given {@literal arrayReference}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public SetIsSubset isSubsetOf(String arrayReference) {
			return createSetIsSubset().isSubsetOf(arrayReference);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns
		 * {@literal true} if it is a subset of the given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public SetIsSubset isSubsetOf(AggregationExpression expression) {
			return createSetIsSubset().isSubsetOf(expression);
		}

		private SetIsSubset createSetIsSubset() {
			return usesFieldRef() ? SetIsSubset.arrayAsSet(fieldReference) : SetIsSubset.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that takes array of the previously mentioned field and returns
		 * {@literal true} if any of the elements are {@literal true} and {@literal false} otherwise.
		 *
		 * @return
		 */
		public AnyElementTrue anyElementTrue() {
			return usesFieldRef() ? AnyElementTrue.arrayAsSet(fieldReference) : AnyElementTrue.arrayAsSet(expression);
		}

		/**
		 * Creates new {@link AggregationExpressions} that tkes array of the previously mentioned field and returns
		 * {@literal true} if no elements is {@literal false}.
		 *
		 * @return
		 */
		public AllElementsTrue allElementsTrue() {
			return usesFieldRef() ? AllElementsTrue.arrayAsSet(fieldReference) : AllElementsTrue.arrayAsSet(expression);
		}

		private boolean usesFieldRef() {
			return this.fieldReference != null;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setEquals}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetEquals extends AbstractAggregationExpression {

		private SetEquals(List<?> arrays) {
			super(arrays);
		}

		@Override
		protected String getMongoMethod() {
			return "$setEquals";
		}

		/**
		 * Create new {@link SetEquals}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static SetEquals arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetEquals(asFields(arrayReference));
		}

		/**
		 * Create new {@link SetEquals}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SetEquals arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetEquals(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link java.util.Set} with all previously added arguments appending the given one.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetEquals isEqualTo(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
			return new SetEquals(append(Fields.fields(arrayReferences).asList()));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetEquals isEqualTo(AggregationExpression... expressions) {

			Assert.notNull(expressions, "Expressions must not be null!");
			return new SetEquals(append(Arrays.asList(expressions)));
		}

		/**
		 * Creates new {@link Sum} with all previously added arguments appending the given one.
		 *
		 * @param array must not be {@literal null}.
		 * @return
		 */
		public SetEquals isEqualTo(Object[] array) {

			Assert.notNull(array, "Array must not be null!");
			return new SetEquals(append(array));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setIntersection}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetIntersection extends AbstractAggregationExpression {

		private SetIntersection(List<?> arrays) {
			super(arrays);
		}

		@Override
		protected String getMongoMethod() {
			return "$setIntersection";
		}

		/**
		 * Creates new {@link SetIntersection}
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static SetIntersection arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetIntersection(asFields(arrayReference));
		}

		/**
		 * Creates new {@link SetIntersection}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SetIntersection arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetIntersection(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link SetIntersection} with all previously added arguments appending the given one.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetIntersection intersects(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
			return new SetIntersection(append(asFields(arrayReferences)));
		}

		/**
		 * Creates new {@link SetIntersection} with all previously added arguments appending the given one.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetIntersection intersects(AggregationExpression... expressions) {

			Assert.notNull(expressions, "Expressions must not be null!");
			return new SetIntersection(append(Arrays.asList(expressions)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setUnion}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetUnion extends AbstractAggregationExpression {

		private SetUnion(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$setUnion";
		}

		/**
		 * Creates new {@link SetUnion}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static SetUnion arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetUnion(asFields(arrayReference));
		}

		/**
		 * Creates new {@link SetUnion}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SetUnion arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetUnion(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link SetUnion} with all previously added arguments appending the given one.
		 *
		 * @param arrayReferences must not be {@literal null}.
		 * @return
		 */
		public SetUnion union(String... arrayReferences) {

			Assert.notNull(arrayReferences, "ArrayReferences must not be null!");
			return new SetUnion(append(asFields(arrayReferences)));
		}

		/**
		 * Creates new {@link SetUnion} with all previously added arguments appending the given one.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		public SetUnion union(AggregationExpression... expressions) {

			Assert.notNull(expressions, "Expressions must not be null!");
			return new SetUnion(append(Arrays.asList(expressions)));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setDifference}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetDifference extends AbstractAggregationExpression {

		private SetDifference(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$setDifference";
		}

		/**
		 * Creates new {@link SetDifference}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static SetDifference arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetDifference(asFields(arrayReference));
		}

		/**
		 * Creates new {@link SetDifference}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SetDifference arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetDifference(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link SetDifference} with all previously added arguments appending the given one.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public SetDifference differenceTo(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetDifference(append(Fields.field(arrayReference)));
		}

		/**
		 * Creates new {@link SetDifference} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public SetDifference differenceTo(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetDifference(append(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $setIsSubset}.
	 *
	 * @author Christoph Strobl
	 */
	public static class SetIsSubset extends AbstractAggregationExpression {

		private SetIsSubset(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$setIsSubset";
		}

		/**
		 * Creates new {@link SetIsSubset}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static SetIsSubset arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetIsSubset(asFields(arrayReference));
		}

		/**
		 * Creates new {@link SetIsSubset}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static SetIsSubset arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetIsSubset(Collections.singletonList(expression));
		}

		/**
		 * Creates new {@link SetIsSubset} with all previously added arguments appending the given one.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public SetIsSubset isSubsetOf(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new SetIsSubset(append(Fields.field(arrayReference)));
		}

		/**
		 * Creates new {@link SetIsSubset} with all previously added arguments appending the given one.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public SetIsSubset isSubsetOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new SetIsSubset(append(expression));
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $anyElementTrue}.
	 *
	 * @author Christoph Strobl
	 */
	public static class AnyElementTrue extends AbstractAggregationExpression {

		private AnyElementTrue(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$anyElementTrue";
		}

		/**
		 * Creates new {@link AnyElementTrue}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static AnyElementTrue arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new AnyElementTrue(asFields(arrayReference));
		}

		/**
		 * Creates new {@link AnyElementTrue}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static AnyElementTrue arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new AnyElementTrue(Collections.singletonList(expression));
		}

		public AnyElementTrue anyElementTrue() {
			return this;
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $allElementsTrue}.
	 *
	 * @author Christoph Strobl
	 */
	public static class AllElementsTrue extends AbstractAggregationExpression {

		private AllElementsTrue(Object value) {
			super(value);
		}

		@Override
		protected String getMongoMethod() {
			return "$allElementsTrue";
		}

		/**
		 * Creates new {@link AllElementsTrue}.
		 *
		 * @param arrayReference must not be {@literal null}.
		 * @return
		 */
		public static AllElementsTrue arrayAsSet(String arrayReference) {

			Assert.notNull(arrayReference, "ArrayReference must not be null!");
			return new AllElementsTrue(asFields(arrayReference));
		}

		/**
		 * Creates new {@link AllElementsTrue}.
		 *
		 * @param expression must not be {@literal null}.
		 * @return
		 */
		public static AllElementsTrue arrayAsSet(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null!");
			return new AllElementsTrue(Collections.singletonList(expression));
		}

		public AllElementsTrue allElementsTrue() {
			return this;
		}
	}
}
