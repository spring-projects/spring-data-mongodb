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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Christoph Strobl
 * @since 1.10
 */
public interface AggregationExpressions {

	/**
	 * @author Christoph Strobl
	 */
	class SetOperators {

		/**
		 * Take the array referenced by given {@literal fieldRef}.
		 *
		 * @param fieldRef must not be {@literal null}.
		 * @return
		 */
		public static SetOperatorFactory arrayAsSet(String fieldRef) {
			return new SetOperatorFactory(fieldRef);
		}

		public static class SetOperatorFactory {

			private final String fieldRef;

			/**
			 * Creates new {@link SetOperatorFactory} for given {@literal fieldRef}.
			 *
			 * @param fieldRef must not be {@literal null}.
			 */
			public SetOperatorFactory(String fieldRef) {

				Assert.notNull(fieldRef, "FieldRef must not be null!");
				this.fieldRef = fieldRef;
			}

			/**
			 * Compares the previously mentioned field to one or more arrays and returns {@literal true} if they have the same
			 * distinct elements and {@literal false} otherwise.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetEquals isEqualTo(String... arrayReferences) {
				return SetEquals.arrayAsSet(fieldRef).isEqualTo(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in every of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetIntersection intersects(String... arrayReferences) {
				return SetIntersection.arrayAsSet(fieldRef).intersects(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and one or more arrays and returns an array that contains the
			 * elements that appear in any of those.
			 *
			 * @param arrayReferences must not be {@literal null}.
			 * @return
			 */
			public SetUnion union(String... arrayReferences) {
				return SetUnion.arrayAsSet(fieldRef).union(arrayReferences);
			}

			/**
			 * Takes array of the previously mentioned field and returns an array containing the elements that do not exist in
			 * the given {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetDifference differenceTo(String arrayReference) {
				return SetDifference.arrayAsSet(fieldRef).differenceTo(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if it is a subset of the given
			 * {@literal arrayReference}.
			 *
			 * @param arrayReference must not be {@literal null}.
			 * @return
			 */
			public SetIsSubset isSubsetOf(String arrayReference) {
				return SetIsSubset.arrayAsSet(fieldRef).isSubsetOf(arrayReference);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if any of the elements are
			 * {@literal true} and {@literal false} otherwise.
			 *
			 * @return
			 */
			public AnyElementTrue anyElementTrue() {
				return AnyElementTrue.arrayAsSet(fieldRef);
			}

			/**
			 * Takes array of the previously mentioned field and returns {@literal true} if no elements is {@literal false}.
			 *
			 * @return
			 */
			public AllElementsTrue allElementsTrue() {
				return AllElementsTrue.arrayAsSet(fieldRef);
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	abstract class AbstractAggregationExpression implements AggregationExpression {

		private final Object value;

		protected AbstractAggregationExpression(Object value) {
			this.value = value;
		}

		@Override
		public DBObject toDbObject(AggregationOperationContext context) {
			return toDbObject(this.value, context);
		}

		public DBObject toDbObject(Object value, AggregationOperationContext context) {

			Object valueToUse;
			if (value instanceof List) {

				List<Object> arguments = (List<Object>) value;
				List<Object> args = new ArrayList<Object>(arguments.size());

				for (Object val : arguments) {
					args.add(unpack(val, context));
				}
				valueToUse = args;
			} else if (value instanceof Map) {

				DBObject dbo = new BasicDBObject();
				for (Map.Entry<String, Object> entry : ((Map<String, Object>) value).entrySet()) {
					dbo.put(entry.getKey(), unpack(entry.getValue(), context));
				}
				valueToUse = dbo;
			}

			else {
				valueToUse = unpack(value, context);
			}

			return new BasicDBObject(getMongoMethod(), valueToUse);
		}

		protected static List<Field> asFields(String... fieldRefs) {

			if (ObjectUtils.isEmpty(fieldRefs)) {
				return Collections.emptyList();
			}

			return Fields.fields(fieldRefs).asList();
		}

		private Object unpack(Object value, AggregationOperationContext context) {

			if (value instanceof AggregationExpression) {
				return ((AggregationExpression) value).toDbObject(context);
			}

			if (value instanceof Field) {
				return context.getReference((Field) value).toString();
			}

			return value;
		}

		protected List<Object> append(Object value) {

			if (this.value instanceof List) {

				List<Object> clone = new ArrayList<Object>((List) this.value);

				if (value instanceof List) {
					for (Object val : (List) value) {
						clone.add(val);
					}
				} else {
					clone.add(value);
				}
				return clone;
			}

			return Arrays.asList(this.value, value);
		}

		protected Object append(String key, Object value) {
			return null;
		}

		public abstract String getMongoMethod();
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetEquals extends AbstractAggregationExpression {

		private SetEquals(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setEquals";
		}

		public static SetEquals arrayAsSet(String arrayReference) {
			return new SetEquals(asFields(arrayReference));
		}

		public SetEquals isEqualTo(String... arrayReferences) {
			return new SetEquals(append(Fields.fields(arrayReferences).asList()));
		}

		public SetEquals isEqualTo(Object[] compareValue) {
			return new SetEquals(append(compareValue));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetIntersection extends AbstractAggregationExpression {

		private SetIntersection(List<?> arrays) {
			super(arrays);
		}

		@Override
		public String getMongoMethod() {
			return "$setIntersection";
		}

		public static SetIntersection arrayAsSet(String arrayReference) {
			return new SetIntersection(asFields(arrayReference));
		}

		public SetIntersection intersects(String... arrayReferences) {
			return new SetIntersection(append(asFields(arrayReferences)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetUnion extends AbstractAggregationExpression {

		private SetUnion(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setUnion";
		}

		public static SetUnion arrayAsSet(String arrayReference) {
			return new SetUnion(asFields(arrayReference));
		}

		public SetUnion union(String... arrayReferences) {
			return new SetUnion(append(asFields(arrayReferences)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetDifference extends AbstractAggregationExpression {

		private SetDifference(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setDifference";
		}

		public static SetDifference arrayAsSet(String arrayReference) {
			return new SetDifference(asFields(arrayReference));
		}

		public SetDifference differenceTo(String arrayReference) {
			return new SetDifference(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class SetIsSubset extends AbstractAggregationExpression {

		private SetIsSubset(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$setIsSubset";
		}

		public static SetIsSubset arrayAsSet(String arrayReference) {
			return new SetIsSubset(asFields(arrayReference));
		}

		public SetIsSubset isSubsetOf(String arrayReference) {
			return new SetIsSubset(append(Fields.field(arrayReference)));
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class AnyElementTrue extends AbstractAggregationExpression {

		private AnyElementTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$anyElementTrue";
		}

		public static AnyElementTrue arrayAsSet(String arrayReference) {
			return new AnyElementTrue(asFields(arrayReference));
		}

		public AnyElementTrue anyElementTrue() {
			return this;
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class AllElementsTrue extends AbstractAggregationExpression {

		private AllElementsTrue(Object value) {
			super(value);
		}

		@Override
		public String getMongoMethod() {
			return "$allElementsTrue";
		}

		public static AllElementsTrue arrayAsSet(String arrayReference) {
			return new AllElementsTrue(asFields(arrayReference));
		}

		public AllElementsTrue allElementsTrue() {
			return this;
		}
	}

	/**
	 * {@code $filter} {@link AggregationExpression} allows to select a subset of the array to return based on the
	 * specified condition.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	class Filter implements AggregationExpression {

		private Object input;
		private ExposedField as;
		private Object condition;

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
		 * @return
		 */
		public static AsBuilder filter(List<?> values) {

			Assert.notNull(values, "Values must not be null!");
			return new FilterExpressionBuilder().filter(values);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
		 */
		@Override
		public DBObject toDbObject(final AggregationOperationContext context) {

			return toFilter(new ExposedFieldsAggregationOperationContext(ExposedFields.from(as), context) {

				@Override
				public FieldReference getReference(Field field) {

					FieldReference ref = null;
					try {
						ref = context.getReference(field);
					} catch (Exception e) {
						// just ignore that one.
					}
					return ref != null ? ref : super.getReference(field);
				}
			});
		}

		private DBObject toFilter(AggregationOperationContext context) {

			DBObject filterExpression = new BasicDBObject();

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("input", getMappedInput(context))));
			filterExpression.put("as", as.getTarget());

			filterExpression.putAll(context.getMappedObject(new BasicDBObject("cond", getMappedCondition(context))));

			return new BasicDBObject("$filter", filterExpression);
		}

		private Object getMappedInput(AggregationOperationContext context) {
			return input instanceof Field ? context.getReference((Field) input).toString() : input;
		}

		private Object getMappedCondition(AggregationOperationContext context) {

			if (!(condition instanceof AggregationExpression)) {
				return condition;
			}

			NestedDelegatingExpressionAggregationOperationContext nea = new NestedDelegatingExpressionAggregationOperationContext(
					context);
			return ((AggregationExpression) condition).toDbObject(nea);
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
			 * @return
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
			 * @return
			 */
			Filter by(AggregationExpression expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(String expression);

			/**
			 * Set the {@literal expression} that determines whether to include the element in the resulting array.
			 *
			 * @param expression must not be {@literal null}.
			 * @return
			 */
			Filter by(DBObject expression);
		}

		/**
		 * @author Christoph Strobl
		 */
		static final class FilterExpressionBuilder implements InputBuilder, AsBuilder, ConditionBuilder {

			private final Filter filter;

			FilterExpressionBuilder() {
				this.filter = new Filter();
			}

			public static InputBuilder newBuilder() {
				return new FilterExpressionBuilder();
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(java.util.List)
			 */
			@Override
			public AsBuilder filter(List<?> array) {

				Assert.notNull(array, "Array must not be null!");
				filter.input = new ArrayList<Object>(array);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.InputBuilder#filter(org.springframework.data.mongodb.core.aggregation.Field)
			 */
			@Override
			public AsBuilder filter(Field field) {

				Assert.notNull(field, "Field must not be null!");
				filter.input = field;
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.AsBuilder#as(java.lang.String)
			 */
			@Override
			public ConditionBuilder as(String variableName) {

				Assert.notNull(variableName, "Variable name  must not be null!");
				filter.as = new ExposedField(variableName, true);
				return this;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(org.springframework.data.mongodb.core.aggregation.AggregationExpression)
			 */
			@Override
			public Filter by(AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null!");
				filter.condition = condition;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(java.lang.String)
			 */
			@Override
			public Filter by(String expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}

			/*
			 * (non-Javadoc)
			 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpressions.Filter.ConditionBuilder#by(com.mongodb.DBObject)
			 */
			@Override
			public Filter by(DBObject expression) {

				Assert.notNull(expression, "Expression must not be null!");
				filter.condition = expression;
				return filter;
			}
		}
	}
}
