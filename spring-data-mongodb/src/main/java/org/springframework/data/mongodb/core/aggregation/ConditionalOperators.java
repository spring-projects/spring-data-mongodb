/*
 * Copyright 2016-2023 the original author or authors.
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
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond.OtherwiseBuilder;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Cond.ThenBuilder;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators.Switch.CaseOperator;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Gateway to {@literal conditional expressions} that evaluate their argument expressions as booleans to a value.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public class ConditionalOperators {

	/**
	 * Take the field referenced by given {@literal fieldReference}.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link ConditionalOperatorFactory}.
	 */
	public static ConditionalOperatorFactory when(String fieldReference) {
		return new ConditionalOperatorFactory(fieldReference);
	}

	/**
	 * Take the value resulting from the given {@literal expression}.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link ConditionalOperatorFactory}.
	 */
	public static ConditionalOperatorFactory when(AggregationExpression expression) {
		return new ConditionalOperatorFactory(expression);
	}

	/**
	 * Take the value resulting from the given {@literal criteriaDefinition}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return new instance of {@link ConditionalOperatorFactory}.
	 */
	public static ConditionalOperatorFactory when(CriteriaDefinition criteriaDefinition) {
		return new ConditionalOperatorFactory(criteriaDefinition);
	}

	/**
	 * Creates new {@link AggregationExpression} that evaluates an expression and returns the value of the expression if
	 * the expression evaluates to a non-null value. If the expression evaluates to a {@literal null} value, including
	 * instances of undefined values or missing fields, returns the value of the replacement expression.
	 *
	 * @param fieldReference must not be {@literal null}.
	 * @return new instance of {@link IfNull.ThenBuilder} to create {@link IfNull}.
	 */
	public static IfNull.ThenBuilder ifNull(String fieldReference) {

		Assert.notNull(fieldReference, "FieldReference must not be null");
		return IfNull.ifNull(fieldReference);
	}

	/**
	 * Creates new {@link AggregationExpression} that evaluates an expression and returns the value of the expression if
	 * the expression evaluates to a non-null value. If the expression evaluates to a {@literal null} value, including
	 * instances of undefined values or missing fields, returns the value of the replacement expression.
	 *
	 * @param expression must not be {@literal null}.
	 * @return new instance of {@link IfNull.ThenBuilder} to create {@link IfNull}.
	 */
	public static IfNull.ThenBuilder ifNull(AggregationExpression expression) {

		Assert.notNull(expression, "Expression must not be null");
		return IfNull.ifNull(expression);
	}

	/**
	 * Creates new {@link AggregationExpression} that evaluates a series of {@link CaseOperator} expressions. When it
	 * finds an expression which evaluates to {@literal true}, {@code $switch} executes a specified expression and breaks
	 * out of the control flow.
	 *
	 * @param conditions must not be {@literal null}.
	 * @return new instance of {@link Switch}.
	 */
	public static Switch switchCases(CaseOperator... conditions) {
		return Switch.switchCases(conditions);
	}

	/**
	 * Creates new {@link AggregationExpression} that evaluates a series of {@link CaseOperator} expressions. When it
	 * finds an expression which evaluates to {@literal true}, {@code $switch} executes a specified expression and breaks
	 * out of the control flow.
	 *
	 * @param conditions must not be {@literal null}.
	 * @return new instance of {@link Switch}.
	 */
	public static Switch switchCases(List<CaseOperator> conditions) {
		return Switch.switchCases(conditions);
	}

	public static class ConditionalOperatorFactory {

		private final @Nullable String fieldReference;

		private final @Nullable AggregationExpression expression;

		private final @Nullable CriteriaDefinition criteriaDefinition;

		/**
		 * Creates new {@link ConditionalOperatorFactory} for given {@literal fieldReference}.
		 *
		 * @param fieldReference must not be {@literal null}.
		 */
		public ConditionalOperatorFactory(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");

			this.fieldReference = fieldReference;
			this.expression = null;
			this.criteriaDefinition = null;
		}

		/**
		 * Creates new {@link ConditionalOperatorFactory} for given {@link AggregationExpression}.
		 *
		 * @param expression must not be {@literal null}.
		 */
		public ConditionalOperatorFactory(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");

			this.fieldReference = null;
			this.expression = expression;
			this.criteriaDefinition = null;
		}

		/**
		 * Creates new {@link ConditionalOperatorFactory} for given {@link CriteriaDefinition}.
		 *
		 * @param criteriaDefinition must not be {@literal null}.
		 */
		public ConditionalOperatorFactory(CriteriaDefinition criteriaDefinition) {

			Assert.notNull(criteriaDefinition, "CriteriaDefinition must not be null");

			this.fieldReference = null;
			this.expression = null;
			this.criteriaDefinition = criteriaDefinition;
		}

		/**
		 * Creates new {@link AggregationExpression} that evaluates a boolean expression to return one of the two specified
		 * return expressions.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link OtherwiseBuilder} to create {@link Cond}.
		 */
		public OtherwiseBuilder then(Object value) {

			Assert.notNull(value, "Value must not be null");
			return createThenBuilder().then(value);
		}

		/**
		 * Creates new {@link AggregationExpression} that evaluates a boolean expression to return one of the two specified
		 * return expressions.
		 *
		 * @param expression must not be {@literal null}.
		 * @return new instance of {@link OtherwiseBuilder} to create {@link Cond}.
		 */
		public OtherwiseBuilder thenValueOf(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return createThenBuilder().then(expression);
		}

		/**
		 * Creates new {@link AggregationExpression} that evaluates a boolean expression to return one of the two specified
		 * return expressions.
		 *
		 * @param fieldReference must not be {@literal null}.
		 * @return new instance of {@link OtherwiseBuilder} to create {@link Cond}.
		 */
		public OtherwiseBuilder thenValueOf(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return createThenBuilder().thenValueOf(fieldReference);
		}

		private ThenBuilder createThenBuilder() {

			if (usesFieldRef()) {
				return Cond.newBuilder().when(fieldReference);
			}

			return usesCriteriaDefinition() ? Cond.newBuilder().when(criteriaDefinition) : Cond.newBuilder().when(expression);
		}

		private boolean usesFieldRef() {
			return this.fieldReference != null;
		}

		private boolean usesCriteriaDefinition() {
			return this.criteriaDefinition != null;
		}
	}

	/**
	 * Encapsulates the aggregation framework {@code $ifNull} operator. Replacement values can be either {@link Field
	 * field references}, {@link AggregationExpression expressions}, values of simple MongoDB types or values that can be
	 * converted to a simple MongoDB type.
	 *
	 * @author Mark Paluch
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/">https://docs.mongodb.com/manual/reference/operator/aggregation/ifNull/</a>
	 */
	public static class IfNull implements AggregationExpression {

		private final Object condition;
		private final Object value;

		private IfNull(Object condition, Object value) {

			this.condition = condition;
			this.value = value;
		}

		/**
		 * Creates new {@link IfNull}.
		 *
		 * @param fieldReference the field to check for a {@literal null} value, field reference must not be
		 *          {@literal null}.
		 * @return never {@literal null}.
		 */
		public static ThenBuilder ifNull(String fieldReference) {

			Assert.notNull(fieldReference, "FieldReference must not be null");
			return new IfNullOperatorBuilder().ifNull(fieldReference);
		}

		/**
		 * Creates new {@link IfNull}.
		 *
		 * @param expression the expression to check for a {@literal null} value, field reference must not be
		 *          {@literal null}.
		 * @return never {@literal null}.
		 */
		public static ThenBuilder ifNull(AggregationExpression expression) {

			Assert.notNull(expression, "Expression must not be null");
			return new IfNullOperatorBuilder().ifNull(expression);
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {

			List<Object> list = new ArrayList<>();

			if (condition instanceof Collection<?> collection) {
				for (Object val : collection) {
					list.add(mapCondition(val, context));
				}
			} else {
				list.add(mapCondition(condition, context));
			}

			list.add(resolve(value, context));
			return new Document("$ifNull", list);
		}

		private Object mapCondition(Object condition, AggregationOperationContext context) {

			if (condition instanceof Field field) {
				return context.getReference(field).toString();
			} else if (condition instanceof AggregationExpression aggregationExpression) {
				return aggregationExpression.toDocument(context);
			} else {
				return condition;
			}
		}

		private Object resolve(Object value, AggregationOperationContext context) {

			if (value instanceof Field field) {
				return context.getReference(field).toString();
			} else if (value instanceof AggregationExpression aggregationExpression) {
				return aggregationExpression.toDocument(context);
			} else if (value instanceof Document) {
				return value;
			}

			return context.getMappedObject(new Document("$set", value)).get("$set");
		}

		/**
		 * @author Mark Paluch
		 */
		public interface IfNullBuilder {

			/**
			 * @param fieldReference the field to check for a {@literal null} value, field reference must not be
			 *          {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder ifNull(String fieldReference);

			/**
			 * @param expression the expression to check for a {@literal null} value, field name must not be {@literal null}
			 *          or empty.
			 * @return the {@link ThenBuilder}.
			 */
			ThenBuilder ifNull(AggregationExpression expression);
		}

		/**
		 * @author Christoph Strobl
		 * @since 3.3
		 */
		public interface OrBuilder {

			/**
			 * @param fieldReference the field to check for a {@literal null} value, field reference must not be
			 *          {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder orIfNull(String fieldReference);

			/**
			 * @param expression the expression to check for a {@literal null} value,
			 * @return the {@link ThenBuilder}.
			 */
			ThenBuilder orIfNull(AggregationExpression expression);
		}

		/**
		 * @author Mark Paluch
		 */
		public interface ThenBuilder extends OrBuilder {

			/**
			 * @param value the value to be used if the {@code $ifNull} condition evaluates {@literal true}. Can be a
			 *          {@link Document}, a value that is supported by MongoDB or a value that can be converted to a MongoDB
			 *          representation but must not be {@literal null}.
			 * @return new instance of {@link IfNull}.
			 */
			IfNull then(Object value);

			/**
			 * @param fieldReference the field holding the replacement value, must not be {@literal null}.
			 * @return new instance of {@link IfNull}.
			 */
			IfNull thenValueOf(String fieldReference);

			/**
			 * @param expression the expression yielding to the replacement value, must not be {@literal null}.
			 * @return new instance of {@link IfNull}.
			 */
			IfNull thenValueOf(AggregationExpression expression);
		}

		/**
		 * Builder for fluent {@link IfNull} creation.
		 *
		 * @author Mark Paluch
		 */
		static final class IfNullOperatorBuilder implements IfNullBuilder, ThenBuilder {

			private @Nullable List<Object> conditions;

			private IfNullOperatorBuilder() {
				conditions = new ArrayList<>();
			}

			/**
			 * Creates a new builder for {@link IfNull}.
			 *
			 * @return never {@literal null}.
			 */
			public static IfNullOperatorBuilder newBuilder() {
				return new IfNullOperatorBuilder();
			}

			public ThenBuilder ifNull(String fieldReference) {

				Assert.hasText(fieldReference, "FieldReference name must not be null or empty");
				this.conditions.add(Fields.field(fieldReference));
				return this;
			}

			@Override
			public ThenBuilder ifNull(AggregationExpression expression) {

				Assert.notNull(expression, "AggregationExpression name must not be null or empty");
				this.conditions.add(expression);
				return this;
			}

			@Override
			public ThenBuilder orIfNull(String fieldReference) {
				return ifNull(fieldReference);
			}

			@Override
			public ThenBuilder orIfNull(AggregationExpression expression) {
				return ifNull(expression);
			}

			public IfNull then(Object value) {
				return new IfNull(conditions, value);
			}

			public IfNull thenValueOf(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null");
				return new IfNull(conditions, Fields.field(fieldReference));
			}

			public IfNull thenValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "Expression must not be null");
				return new IfNull(conditions, expression);
			}
		}
	}

	/**
	 * {@link AggregationExpression} for {@code $switch}.
	 *
	 * @author Christoph Strobl
	 */
	public static class Switch extends AbstractAggregationExpression {

		private Switch(java.util.Map<String, Object> values) {
			super(values);
		}

		@Override
		protected String getMongoMethod() {
			return "$switch";
		}

		/**
		 * Creates new {@link Switch}.
		 *
		 * @param conditions must not be {@literal null}.
		 */
		public static Switch switchCases(CaseOperator... conditions) {

			Assert.notNull(conditions, "Conditions must not be null");
			return switchCases(Arrays.asList(conditions));
		}

		/**
		 * Creates new {@link Switch}.
		 *
		 * @param conditions must not be {@literal null}.
		 */
		public static Switch switchCases(List<CaseOperator> conditions) {

			Assert.notNull(conditions, "Conditions must not be null");
			return new Switch(Collections.singletonMap("branches", new ArrayList<>(conditions)));
		}

		/**
		 * Set the default value.
		 *
		 * @param value must not be {@literal null}.
		 * @return new instance of {@link Switch}.
		 */
		public Switch defaultTo(Object value) {
			return new Switch(append("default", value));
		}

		/**
		 * Encapsulates the aggregation framework case document inside a {@code $switch}-operation.
		 */
		public static class CaseOperator implements AggregationExpression {

			private final AggregationExpression when;
			private final Object then;

			private CaseOperator(AggregationExpression when, Object then) {

				this.when = when;
				this.then = then;
			}

			public static ThenBuilder when(final AggregationExpression condition) {

				Assert.notNull(condition, "Condition must not be null");

				return new ThenBuilder() {

					@Override
					public CaseOperator then(Object value) {

						Assert.notNull(value, "Value must not be null");
						return new CaseOperator(condition, value);
					}
				};
			}

			@Override
			public Document toDocument(AggregationOperationContext context) {

				Document dbo = new Document("case", when.toDocument(context));

				if (then instanceof AggregationExpression aggregationExpression) {
					dbo.put("then", aggregationExpression.toDocument(context));
				} else if (then instanceof Field field) {
					dbo.put("then", context.getReference(field).toString());
				} else {
					dbo.put("then", then);
				}

				return dbo;
			}

			/**
			 * @author Christoph Strobl
			 */
			public interface ThenBuilder {

				/**
				 * Set the then {@literal value}.
				 *
				 * @param value must not be {@literal null}.
				 * @return new instance of {@link CaseOperator}.
				 */
				CaseOperator then(Object value);
			}
		}
	}

	/**
	 * Encapsulates the aggregation framework {@code $cond} operator. A {@link Cond} allows nested conditions
	 * {@code if-then[if-then-else]-else} using {@link Field}, {@link CriteriaDefinition}, {@link AggregationExpression}
	 * or a {@link Document custom} condition. Replacement values can be either {@link Field field references},
	 * {@link AggregationExpression expressions}, values of simple MongoDB types or values that can be converted to a
	 * simple MongoDB type.
	 *
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/reference/operator/aggregation/cond/">https://docs.mongodb.com/manual/reference/operator/aggregation/cond/</a>
	 */
	public static class Cond implements AggregationExpression {

		private final Object condition;
		private final Object thenValue;
		private final Object otherwiseValue;

		/**
		 * Creates a new {@link Cond} for a given {@link Field} and {@code then}/{@code otherwise} values.
		 *
		 * @param condition must not be {@literal null}.
		 * @param thenValue must not be {@literal null}.
		 * @param otherwiseValue must not be {@literal null}.
		 */
		private Cond(Field condition, Object thenValue, Object otherwiseValue) {
			this((Object) condition, thenValue, otherwiseValue);
		}

		/**
		 * Creates a new {@link Cond} for a given {@link CriteriaDefinition} and {@code then}/{@code otherwise} values.
		 *
		 * @param condition must not be {@literal null}.
		 * @param thenValue must not be {@literal null}.
		 * @param otherwiseValue must not be {@literal null}.
		 */
		private Cond(CriteriaDefinition condition, Object thenValue, Object otherwiseValue) {
			this((Object) condition, thenValue, otherwiseValue);
		}

		private Cond(Object condition, Object thenValue, Object otherwiseValue) {

			Assert.notNull(condition, "Condition must not be null");
			Assert.notNull(thenValue, "Then value must not be null");
			Assert.notNull(otherwiseValue, "Otherwise value must not be null");

			assertNotBuilder(condition, "Condition");
			assertNotBuilder(thenValue, "Then value");
			assertNotBuilder(otherwiseValue, "Otherwise value");

			this.condition = condition;
			this.thenValue = thenValue;
			this.otherwiseValue = otherwiseValue;
		}

		@Override
		public Document toDocument(AggregationOperationContext context) {

			Document condObject = new Document();

			condObject.append("if", resolveCriteria(context, condition));
			condObject.append("then", resolveValue(context, thenValue));
			condObject.append("else", resolveValue(context, otherwiseValue));

			return new Document("$cond", condObject);
		}

		private Object resolveValue(AggregationOperationContext context, Object value) {

			if (value instanceof Document || value instanceof Field) {
				return resolve(context, value);
			}

			if (value instanceof AggregationExpression aggregationExpression) {
				return aggregationExpression.toDocument(context);
			}

			return context.getMappedObject(new Document("$set", value)).get("$set");
		}

		private Object resolveCriteria(AggregationOperationContext context, Object value) {

			if (value instanceof Document || value instanceof Field) {
				return resolve(context, value);
			}

			if (value instanceof AggregationExpression aggregationExpression) {
				return aggregationExpression.toDocument(context);
			}

			if (value instanceof CriteriaDefinition criteriaDefinition) {

				Document mappedObject = context.getMappedObject(criteriaDefinition.getCriteriaObject());
				List<Object> clauses = getClauses(context, mappedObject);
				return clauses.size() == 1 ? clauses.get(0) : clauses;
			}

			throw new InvalidDataAccessApiUsageException(
					String.format("Invalid value in condition; Supported: Document, Field references, Criteria, got: %s", value));
		}

		private List<Object> getClauses(AggregationOperationContext context, Document mappedObject) {

			List<Object> clauses = new ArrayList<>();

			for (String key : mappedObject.keySet()) {

				Object predicate = mappedObject.get(key);
				clauses.addAll(getClauses(context, key, predicate));
			}

			return clauses;
		}

		private List<Object> getClauses(AggregationOperationContext context, String key, Object predicate) {

			List<Object> clauses = new ArrayList<>();

			if (predicate instanceof List<?> predicates) {

				List<Object> args = new ArrayList<>(predicates.size());

				for (Object clause : predicates) {
					if (clause instanceof Document document) {
						args.addAll(getClauses(context, document));
					}
				}

				clauses.add(new Document(key, args));
			} else if (predicate instanceof Document nested) {

				for (String s : nested.keySet()) {

					if (!isKeyword(s)) {
						continue;
					}

					List<Object> args = new ArrayList<>(2);
					args.add("$" + key);
					args.add(nested.get(s));
					clauses.add(new Document(s, args));
				}
			} else if (!isKeyword(key)) {

				List<Object> args = new ArrayList<>(2);
				args.add("$" + key);
				args.add(predicate);
				clauses.add(new Document("$eq", args));
			}

			return clauses;
		}

		/**
		 * Returns whether the given {@link String} is a MongoDB keyword.
		 *
		 * @param candidate
		 * @return
		 */
		private boolean isKeyword(String candidate) {
			return candidate.startsWith("$");
		}

		private Object resolve(AggregationOperationContext context, Object value) {

			if (value instanceof Document document) {
				return context.getMappedObject(document);
			}

			return context.getReference((Field) value).toString();
		}

		private void assertNotBuilder(Object toCheck, String name) {
			Assert.isTrue(!ClassUtils.isAssignableValue(ConditionalExpressionBuilder.class, toCheck),
					String.format("%s must not be of type %s", name, ConditionalExpressionBuilder.class.getSimpleName()));
		}

		/**
		 * Get a builder that allows fluent creation of {@link Cond}.
		 *
		 * @return never {@literal null}.
		 */
		public static WhenBuilder newBuilder() {
			return ConditionalExpressionBuilder.newBuilder();
		}

		/**
		 * Start creating new {@link Cond} by providing the boolean expression used in {@code if}.
		 *
		 * @param booleanExpression must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static ThenBuilder when(Document booleanExpression) {
			return ConditionalExpressionBuilder.newBuilder().when(booleanExpression);
		}

		/**
		 * Start creating new {@link Cond} by providing the {@link AggregationExpression} used in {@code if}.
		 *
		 * @param expression expression that yields in a boolean result, must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static ThenBuilder when(AggregationExpression expression) {
			return ConditionalExpressionBuilder.newBuilder().when(expression);
		}

		/**
		 * Start creating new {@link Cond} by providing the field reference used in {@code if}.
		 *
		 * @param booleanField name of a field holding a boolean value, must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		public static ThenBuilder when(String booleanField) {
			return ConditionalExpressionBuilder.newBuilder().when(booleanField);
		}

		/**
		 * Start creating new {@link Cond} by providing the {@link CriteriaDefinition} used in {@code if}.
		 *
		 * @param criteria criteria to evaluate, must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		public static ThenBuilder when(CriteriaDefinition criteria) {
			return ConditionalExpressionBuilder.newBuilder().when(criteria);
		}

		/**
		 * @author Mark Paluch
		 */
		public interface WhenBuilder {

			/**
			 * @param booleanExpression expression that yields in a boolean result, must not be {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder when(Document booleanExpression);

			/**
			 * @param expression expression that yields in a boolean result, must not be {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder when(AggregationExpression expression);

			/**
			 * @param booleanField name of a field holding a boolean value, must not be {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder when(String booleanField);

			/**
			 * @param criteria criteria to evaluate, must not be {@literal null}.
			 * @return the {@link ThenBuilder}
			 */
			ThenBuilder when(CriteriaDefinition criteria);
		}

		/**
		 * @author Mark Paluch
		 */
		public interface ThenBuilder {

			/**
			 * @param value the value to be used if the condition evaluates {@literal true}. Can be a {@link Document}, a
			 *          value that is supported by MongoDB or a value that can be converted to a MongoDB representation but
			 *          must not be {@literal null}.
			 * @return the {@link OtherwiseBuilder}
			 */
			OtherwiseBuilder then(Object value);

			/**
			 * @param fieldReference must not be {@literal null}.
			 * @return the {@link OtherwiseBuilder}
			 */
			OtherwiseBuilder thenValueOf(String fieldReference);

			/**
			 * @param expression must not be {@literal null}.
			 * @return the {@link OtherwiseBuilder}
			 */
			OtherwiseBuilder thenValueOf(AggregationExpression expression);
		}

		/**
		 * @author Mark Paluch
		 */
		public interface OtherwiseBuilder {

			/**
			 * @param value the value to be used if the condition evaluates {@literal false}. Can be a {@link Document}, a
			 *          value that is supported by MongoDB or a value that can be converted to a MongoDB representation but
			 *          must not be {@literal null}.
			 * @return the {@link Cond}
			 */
			Cond otherwise(Object value);

			/**
			 * @param fieldReference must not be {@literal null}.
			 * @return the {@link Cond}
			 */
			Cond otherwiseValueOf(String fieldReference);

			/**
			 * @param expression must not be {@literal null}.
			 * @return the {@link Cond}
			 */
			Cond otherwiseValueOf(AggregationExpression expression);
		}

		/**
		 * Builder for fluent {@link Cond} creation.
		 *
		 * @author Mark Paluch
		 */
		static class ConditionalExpressionBuilder implements WhenBuilder, ThenBuilder, OtherwiseBuilder {

			private @Nullable Object condition;
			private @Nullable Object thenValue;

			private ConditionalExpressionBuilder() {}

			/**
			 * Creates a new builder for {@link Cond}.
			 *
			 * @return never {@literal null}.
			 */
			public static ConditionalExpressionBuilder newBuilder() {
				return new ConditionalExpressionBuilder();
			}

			@Override
			public ConditionalExpressionBuilder when(Document booleanExpression) {

				Assert.notNull(booleanExpression, "'Boolean expression' must not be null");

				this.condition = booleanExpression;
				return this;
			}

			@Override
			public ThenBuilder when(CriteriaDefinition criteria) {

				Assert.notNull(criteria, "Criteria must not be null");
				this.condition = criteria;
				return this;
			}

			@Override
			public ThenBuilder when(AggregationExpression expression) {

				Assert.notNull(expression, "AggregationExpression field must not be null");
				this.condition = expression;
				return this;
			}

			@Override
			public ThenBuilder when(String booleanField) {

				Assert.hasText(booleanField, "Boolean field name must not be null or empty");
				this.condition = Fields.field(booleanField);
				return this;
			}

			@Override
			public OtherwiseBuilder then(Object thenValue) {

				Assert.notNull(thenValue, "Then-value must not be null");
				this.thenValue = thenValue;
				return this;
			}

			@Override
			public OtherwiseBuilder thenValueOf(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null");
				this.thenValue = Fields.field(fieldReference);
				return this;
			}

			@Override
			public OtherwiseBuilder thenValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "AggregationExpression must not be null");
				this.thenValue = expression;
				return this;
			}

			@Override
			public Cond otherwise(Object otherwiseValue) {

				Assert.notNull(otherwiseValue, "Value must not be null");
				return new Cond(condition, thenValue, otherwiseValue);
			}

			@Override
			public Cond otherwiseValueOf(String fieldReference) {

				Assert.notNull(fieldReference, "FieldReference must not be null");
				return new Cond(condition, thenValue, Fields.field(fieldReference));
			}

			@Override
			public Cond otherwiseValueOf(AggregationExpression expression) {

				Assert.notNull(expression, "AggregationExpression must not be null");
				return new Cond(condition, thenValue, expression);
			}
		}
	}
}
